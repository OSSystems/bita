use atty::Stream;
use blake2::{Blake2b, Digest};
use buzhash::BuzHash;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use threadpool::ThreadPool;

use archive;
use archive_reader::*;
use chunk_dictionary;
use chunker::Chunker;
use chunker_utils::*;
use config::*;
use errors::*;
use local_reader_backend::LocalReaderBackend;
use remote_archive_backend::RemoteReader;
use seed;
use std::io::BufWriter;
use std::os::linux::fs::MetadataExt;
use string_utils::*;

// Scan seeds and clone matching chunks
fn clone_from_seeds<F>(
    pool: &ThreadPool,
    archive: &ArchiveReader,
    seed_files: &[PathBuf],
    chunks_left: &mut HashSet<HashBuf>,
    mut seed_output: F,
) -> Result<()>
where
    F: FnMut(
        &HashBuf,
        seed::DataVerified,
        chunk_dictionary::ChunkCompression_CompressionType,
        Vec<u8>,
    ) -> Result<()>,
{
    // Setup chunker to use when chunking seed input
    let chunker = Chunker::new(
        1024 * 1024,
        archive.chunk_filter_bits,
        archive.min_chunk_size,
        archive.max_chunk_size,
        BuzHash::new(archive.hash_window_size as usize, ::BUZHASH_SEED),
    );

    // Run input seed files through chunker and use chunks which are in the target file.
    // Start with scanning stdin, if not a tty.
    if !atty::is(Stream::Stdin) {
        let stdin = io::stdin();
        println!("Scanning stdin for chunks...");
        let chunks_missing = chunks_left.len();
        let mut chunker = chunker.clone();
        seed::from_stream(
            stdin.lock(),
            &mut chunker,
            archive.hash_length,
            chunks_left,
            &mut seed_output,
            &pool,
        )?;
        println!(
            "Used {} chunks from stdin",
            chunks_missing - chunks_left.len()
        );
    }
    // Now scan through all given seed files
    for file_path in seed_files {
        if !chunks_left.is_empty() {
            let chunks_missing = chunks_left.len();
            println!("Scanning {} for chunks...", file_path.display());
            seed::from_file(
                file_path,
                chunker.clone(),
                archive.hash_length,
                chunks_left,
                &mut seed_output,
                &pool,
            )?;
            println!(
                "Used {} chunks from seed file {}",
                chunks_missing - chunks_left.len(),
                file_path.display(),
            );
        }
    }

    Ok(())
}

fn clone_input<T>(mut input: T, config: &CloneConfig, pool: &ThreadPool) -> Result<()>
where
    T: ArchiveBackend,
{
    let archive = ArchiveReader::try_init(&mut input, &mut Vec::new())?;
    let mut chunks_left = archive.chunk_hash_set();

    println!("Cloning {}", archive);

    // Create or open output file.
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(config.base.force_create)
        .create_new(!config.base.force_create)
        .open(&config.output)
        .chain_err(|| format!("failed to open output file ({})", config.output))?;

    // Check if the given output file is a regular file or block device.
    // If it is a block device we should check its size against the target size before
    // writing. If a regular file then resize that file to target size.
    let meta = output_file
        .metadata()
        .chain_err(|| "unable to get file meta data")?;
    if meta.st_mode() & 0x6000 == 0x6000 {
        // Output is a block device
        let size = output_file
            .seek(SeekFrom::End(0))
            .chain_err(|| "unable to seek output file")?;
        if size != archive.source_total_size {
            panic!(
                "Size of output ({}) differ from size of archive target file ({})",
                size_to_str(size),
                size_to_str(archive.source_total_size)
            );
        }
        output_file
            .seek(SeekFrom::Start(0))
            .chain_err(|| "unable to seek output file")?;
    } else {
        // Output is a reqular file
        output_file
            .set_len(archive.source_total_size)
            .chain_err(|| "unable to resize output file")?;
    }

    let mut output_file = BufWriter::new(output_file);
    let mut chunk_data_hasher = Blake2b::new();

    // Chunk output closure
    let mut chunk_counter = 0;
    let mut chunk_output = |expected_checksum: &HashBuf,
                            chunk_is_from_seed: Option<seed::DataVerified>,
                            compression: chunk_dictionary::ChunkCompression_CompressionType,
                            raw_chunk_data: Vec<u8>| {
        // Decompress the raw chunk data
        let raw_size = raw_chunk_data.len();
        let mut chunk_data = vec![];

        archive::decompress_chunk(compression, raw_chunk_data, &mut chunk_data)?;
        chunk_counter += 1;

        if chunk_is_from_seed == Some(seed::DataVerified::Yes) {
            // Chunk data is from seed and has already been verified
        } else {
            // Chunk data is not verified
            chunk_data_hasher.input(&chunk_data);
            let checksum = chunk_data_hasher.result_reset().to_vec();
            if checksum[..expected_checksum.len()] != expected_checksum[..] {
                bail!(
                    "Chunk hash mismatch (expected: {}, got: {})",
                    HexSlice::new(expected_checksum),
                    HexSlice::new(&checksum[0..expected_checksum.len()])
                );
            }
        }

        // Write the chunk to output filex
        let source_offsets = archive.chunk_source_offsets(expected_checksum);
        for offset in &source_offsets {
            output_file
                .seek(SeekFrom::Start(*offset as u64))
                .expect("seek output");
            output_file.write_all(&chunk_data).expect("write output");
        }

        println!(
            "Chunk {} '{}' from {} of size {} decompressed to {}",
            //archive.num_chunks(),
            chunk_counter,
            HexSlice::new(expected_checksum),
            match chunk_is_from_seed {
                Some(_) => "seed",
                None => "archive",
            },
            size_to_str(raw_size),
            size_to_str(chunk_data.len() * source_offsets.len()),
        );

        Ok(())
    };

    let mut bytes_from_seed = 0;

    // Clone chunks from seed files
    clone_from_seeds(
        pool,
        &archive,
        &config.seed_files,
        &mut chunks_left,
        |expected_checksum: &HashBuf,
         chunk_data_verified: seed::DataVerified,
         compression: chunk_dictionary::ChunkCompression_CompressionType,
         raw_chunk_data: Vec<u8>| {
            // Got chunk data from seed
            bytes_from_seed += raw_chunk_data.len();

            chunk_output(
                expected_checksum,
                Some(chunk_data_verified),
                compression,
                raw_chunk_data,
            )
        },
    )?;

    // Clone rest of the chunks from archive
    let bytes_from_archive =
        archive.read_chunk_data(input, &chunks_left, |chunk_descriptor, raw_chunk_data| {
            let expected_checksum = &chunk_descriptor.checksum;

            chunk_output(
                expected_checksum,
                None,
                chunk_descriptor.compression.get_compression(),
                raw_chunk_data,
            )
        })?;

    println!(
        "Cloned using {} from seed and {} from archive.",
        size_to_str(bytes_from_seed),
        size_to_str(bytes_from_archive)
    );

    Ok(())
}

pub fn run(config: &CloneConfig, pool: &ThreadPool) -> Result<()> {
    if &config.input[0..7] == "http://" || &config.input[0..8] == "https://" {
        let remote_source = RemoteReader::new(&config.input);
        clone_input(remote_source, config, pool)?;
    } else {
        let local_file = LocalReaderBackend::new(&Path::new(&config.input));
        clone_input(local_file, config, pool)?;
    }
    Ok(())
}
