use atty::Stream;
use blake2::{Blake2b, Digest};
use buzhash::BuzHash;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::Path;
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
use std::io::BufWriter;
use std::os::linux::fs::MetadataExt;
use string_utils::*;

#[derive(PartialEq)]
enum SeedDataVerified {
    Yes,
    No,
}

fn seed_from_stream<T, F>(
    mut seed_input: T,
    chunker: &mut Chunker,
    hash_length: usize,
    chunk_hash_set: &mut HashSet<HashBuf>,
    mut chunk_data_callback: F,
    pool: &ThreadPool,
) -> Result<()>
where
    T: Read,
    F: FnMut(
        &HashBuf,
        SeedDataVerified,
        chunk_dictionary::ChunkCompression_CompressionType,
        Vec<u8>,
    ) -> Result<()>,
{
    // Generate strong hash for a chunk
    let hasher = |data: &[u8]| {
        let mut hasher = Blake2b::new();
        hasher.input(data);
        hasher.result().to_vec()
    };
    unique_chunks(
        &mut seed_input,
        chunker,
        hasher,
        &pool,
        false,
        |hashed_chunk| {
            let hash = &hashed_chunk.hash[0..hash_length].to_vec();
            if chunk_hash_set.contains(hash) {
                chunk_data_callback(
                    hash,
                    SeedDataVerified::Yes,
                    chunk_dictionary::ChunkCompression_CompressionType::NONE,
                    hashed_chunk.data,
                )
                .expect("write to output from seed");
                chunk_hash_set.remove(hash);
            }
        },
    )
    .chain_err(|| "failed to get unique chunks")?;

    println!(
        "Chunker - scan time: {}.{:03} s, read time: {}.{:03} s",
        chunker.scan_time.as_secs(),
        chunker.scan_time.subsec_millis(),
        chunker.read_time.as_secs(),
        chunker.read_time.subsec_millis()
    );
    Ok(())
}

fn seed_from_file<F>(
    file_path: &Path,
    mut chunker: Chunker,
    hash_length: usize,
    chunk_hash_set: &mut HashSet<HashBuf>,
    mut chunk_data_callback: F,
    pool: &ThreadPool,
) -> Result<()>
where
    F: FnMut(
        &HashBuf,
        SeedDataVerified,
        chunk_dictionary::ChunkCompression_CompressionType,
        Vec<u8>,
    ) -> Result<()>,
{
    // Test if the given seed can be read as a bita archive.
    let mut seed_input = File::open(file_path)
        .chain_err(|| format!("failed to open seed file ({})", file_path.display()))?;

    let mut archive_header = Vec::new();
    match ArchiveReader::try_init(&mut seed_input, &mut archive_header) {
        Err(Error(ErrorKind::NotAnArchive(_), _)) => {
            // As the input file was not an archive we feed the data read so
            // far into the chunker.
            chunker.preload(&archive_header);

            seed_from_stream(
                seed_input,
                &mut chunker,
                hash_length,
                chunk_hash_set,
                chunk_data_callback,
                pool,
            )
        }
        Err(err) => Err(err),
        Ok(ref mut archive) => {
            // Close the seed input file and create a remote backend using the file path
            drop(seed_input);

            let seed_input = LocalReaderBackend::new(file_path);

            // Is an archive
            archive.read_chunk_data(
                seed_input,
                &chunk_hash_set.clone(),
                |chunk_descriptor, chunk_data| {
                    // Got chunk data for a matching chunk
                    chunk_data_callback(
                        &chunk_descriptor.checksum,
                        SeedDataVerified::No,
                        chunk_descriptor.compression.get_compression(),
                        chunk_data,
                    )?;
                    chunk_hash_set.remove(&chunk_descriptor.checksum);
                    Ok(())
                },
            )?;

            Ok(())
        }
    }
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

    // Setup chunker to use when chunking seed input
    let chunker = Chunker::new(
        1024 * 1024,
        archive.chunk_filter_bits,
        archive.min_chunk_size,
        archive.max_chunk_size,
        BuzHash::new(archive.hash_window_size as usize, ::BUZHASH_SEED),
    );

    let mut total_read_from_seed = 0;
    let mut total_from_archive = 0;
    let mut chunk_data_hasher = Blake2b::new();
    {
        // Closure for writing chunks to output

        let mut seed_output = |expected_checksum: &HashBuf,
                               seed_data_verified: SeedDataVerified,
                               compression: chunk_dictionary::ChunkCompression_CompressionType,
                               raw_chunk_data: Vec<u8>| {
            // Got chunk data from seed
            total_read_from_seed += raw_chunk_data.len();

            // Decompress the raw chunk data
            let mut chunk_data = vec![];
            archive::decompress_chunk(compression, raw_chunk_data, &mut chunk_data)?;

            if seed_data_verified == SeedDataVerified::No {
                // Verify data by hash
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

            for offset in archive.chunk_source_offsets(expected_checksum) {
                output_file
                    .seek(SeekFrom::Start(offset as u64))
                    .expect("seek output");
                output_file.write_all(&chunk_data).expect("write output");
            }

            println!(
                "Chunk '{}', size {} used from seed",
                HexSlice::new(expected_checksum),
                size_to_str(chunk_data.len()),
            );

            Ok(())
        };

        // Run input seed files through chunker and use chunks which are in the target file.
        // Start with scanning stdin, if not a tty.
        if !atty::is(Stream::Stdin) {
            let stdin = io::stdin();
            println!("Scanning stdin for chunks...");
            let mut chunker = chunker.clone();
            seed_from_stream(
                stdin.lock(),
                &mut chunker,
                archive.hash_length,
                &mut chunks_left,
                &mut seed_output,
                &pool,
            )?;
            println!(
                "Reached end of stdin ({} chunks missing)",
                chunks_left.len()
            );
        }
        // Now scan through all given seed files
        for file_path in &config.seed_files {
            if !chunks_left.is_empty() {
                println!("Scanning {} for chunks...", file_path.display());
                seed_from_file(
                    file_path,
                    chunker.clone(),
                    archive.hash_length,
                    &mut chunks_left,
                    &mut seed_output,
                    &pool,
                )?;
                println!(
                    "Reached end of {} ({} chunks missing)",
                    file_path.display(),
                    chunks_left.len()
                );
            }
        }
    }

    // Clone rest of the chunks from archive
    let archive_total_read =
        archive.read_chunk_data(input, &chunks_left, |chunk_descriptor, raw_chunk_data| {
            let expected_checksum = &chunk_descriptor.checksum;

            // Decompress the raw chunk data
            let mut chunk_data = vec![];
            archive::decompress_chunk(
                chunk_descriptor.compression.get_compression(),
                raw_chunk_data,
                &mut chunk_data,
            )?;

            // Verify data by hash
            chunk_data_hasher.input(&chunk_data);
            let checksum = chunk_data_hasher.result_reset().to_vec();
            if checksum[..expected_checksum.len()] != expected_checksum[..] {
                bail!(
                    "Chunk hash mismatch (expected: {}, got: {})",
                    HexSlice::new(expected_checksum),
                    HexSlice::new(&checksum[0..expected_checksum.len()])
                );
            }

            let offsets = &archive.chunk_source_offsets(&chunk_descriptor.checksum);
            for offset in offsets {
                total_from_archive += chunk_data.len();
                output_file
                    .seek(SeekFrom::Start(*offset as u64))
                    .chain_err(|| "failed to seek output file")?;
                output_file
                    .write_all(&chunk_data)
                    .chain_err(|| "failed to write output file")?;
            }

            match chunk_descriptor.compression.get_compression() {
                chunk_dictionary::ChunkCompression_CompressionType::NONE => println!(
                    "Chunk '{}', size {}, uncompressed, insert at {:?}",
                    HexSlice::new(&chunk_descriptor.checksum),
                    size_to_str(chunk_data.len()),
                    offsets
                ),
                _ => println!(
                    "Chunk '{}', size {}, decompressed to {}, insert at {:?}",
                    HexSlice::new(&chunk_descriptor.checksum),
                    size_to_str(chunk_descriptor.stored_size),
                    size_to_str(chunk_data.len()),
                    offsets
                ),
            }

            Ok(())
        })?;

    println!(
        "Cloned using {} from seed and {} from archive.",
        size_to_str(total_read_from_seed),
        size_to_str(archive_total_read)
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
