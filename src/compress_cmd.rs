use atty::Stream;
use blake2::{Blake2b, Digest};
use lzma::LzmaWriter;
use protobuf::{RepeatedField, SingularPtrField};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom, Write};
use string_utils::*;
use threadpool::ThreadPool;

use archive;
use buzhash::BuzHash;
use chunk_dictionary;
use chunker::*;
use chunker_utils::*;
use config::*;
use errors::*;

// Ok((file_size, file_hash, chunks, chunk_descriptors))
struct ChunkFileDescriptor {
    total_file_size: usize,
    file_hash: HashBuf,
    chunk_descriptors: Vec<chunk_dictionary::ChunkDescriptor>,
    chunk_order: Vec<ChunkSourceDescriptor>,
}

fn chunk_into_file(
    config: &CompressConfig,
    pool: &ThreadPool,
    chunk_file: &mut File,
) -> Result<ChunkFileDescriptor> {
    // Setup the chunker
    let mut chunker = Chunker::new(
        1024 * 1024,
        config.chunk_filter_bits,
        config.min_chunk_size,
        config.max_chunk_size,
        BuzHash::new(config.hash_window_size as usize, ::BUZHASH_SEED),
    );

    let mut compression = chunk_dictionary::ChunkCompression::new();
    compression.set_compression(config.compression);
    compression.set_compression_level(config.compression_level);

    // Compress a chunk
    let compression_level = config.compression_level;
    let compression_type = config.compression;
    let chunk_compressor = move |data: &[u8]| -> Vec<u8> {
        match compression_type {
            chunk_dictionary::ChunkCompression_CompressionType::LZMA => {
                let mut result = vec![];
                {
                    let mut f = LzmaWriter::new_compressor(&mut result, compression_level)
                        .expect("new lzma compressor");
                    f.write_all(data).expect("write compressor");
                    f.finish().expect("finish compressor");
                }
                result
            }
            chunk_dictionary::ChunkCompression_CompressionType::ZSTD => {
                let mut result = vec![];
                let mut data = data.to_vec();
                zstd::stream::copy_encode(&data[..], &mut result, compression_level as i32)
                    .expect("zstd compressor");
                result
            }
            chunk_dictionary::ChunkCompression_CompressionType::NONE => data.to_vec(),
        }
    };

    // Generate strong hash for a chunk
    fn hasher(data: &[u8]) -> Vec<u8> {
        let mut h = Blake2b::new();
        h.input(data);
        h.result().to_vec()
    };

    let mut total_compressed_size = 0;
    let mut total_unique_chunks = 0;
    let mut total_unique_chunk_size = 0;
    let mut archive_offset: u64 = 0;
    let mut chunk_descriptors = Vec::new();
    let chunk_order;
    let total_file_size;
    let file_hash;
    {
        let process_chunk = |comp_chunk: CompressedChunk| {
            // For each unique and compressed chunk
            let chunk_data;
            let hash = &comp_chunk.hash[0..config.hash_length as usize];
            let use_compression = if comp_chunk.cdata.len() < comp_chunk.data.len() {
                // Use the compressed data
                chunk_data = &comp_chunk.cdata;
                Some(compression.clone())
            } else {
                // Compressed chunk bigger than raw - Use raw
                chunk_data = &comp_chunk.data;
                None
            };

            println!(
                "Chunk {}, '{}', offset: {}, size: {}, compressed to: {}, compression: {}",
                total_unique_chunks,
                HexSlice::new(&hash),
                comp_chunk.offset,
                size_to_str(comp_chunk.data.len()),
                size_to_str(comp_chunk.cdata.len()),
                match use_compression {
                    None => "none".to_owned(),
                    Some(ref v) => format!("{}", v),
                },
            );

            total_unique_chunks += 1;
            total_unique_chunk_size += comp_chunk.data.len();
            total_compressed_size += chunk_data.len();

            // Store a chunk descriptor which referes to the compressed data
            chunk_descriptors.push(chunk_dictionary::ChunkDescriptor {
                checksum: hash.to_vec(),
                size: comp_chunk.data.len() as u64,
                compressed_size: chunk_data.len() as u64,
                archive_offset: 0,
                location_index: 0,
                compression: protobuf::SingularPtrField::from_option(use_compression),
                unknown_fields: std::default::Default::default(),
                cached_size: std::default::Default::default(),
            });

            // Create chunk store file
            let compression_type_str = match compression_type {
                chunk_dictionary::ChunkCompression_CompressionType::LZMA => "lzma",
                chunk_dictionary::ChunkCompression_CompressionType::ZSTD => "zstd",
                chunk_dictionary::ChunkCompression_CompressionType::NONE => "none",
            };
            let chunk_file_path = config
                .chunk_store_path
                .join(buf_to_hex_str(hash))
                .with_extension(compression_type_str.to_string() + ".chunk");

            // TODO: If file exist - overwrite or verify the content and leave as is?
            let mut chunk_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&chunk_file_path)
                .chain_err(|| {
                    format!(
                        "unable to create chunk file ({})",
                        chunk_file_path.display()
                    )
                })
                .expect("create chunk file");

            chunk_file.write_all(chunk_data).expect("write chunk");
            archive_offset += chunk_data.len() as u64;
        };

        if let Some(ref input_path) = config.input {
            // Read source from file
            let mut src_file = File::open(&input_path)
                .chain_err(|| format!("unable to open input file ({})", input_path.display()))?;

            let (tmp_file_size, tmp_file_hash, tmp_chunks) = unique_compressed_chunks(
                &mut src_file,
                &mut chunker,
                hasher,
                chunk_compressor,
                &pool,
                true,
                process_chunk,
            )
            .chain_err(|| "unable to compress chunk")?;
            total_file_size = tmp_file_size;
            file_hash = tmp_file_hash;
            chunk_order = tmp_chunks;
        } else if !atty::is(Stream::Stdin) {
            // Read source from stdin
            let stdin = io::stdin();
            let mut src_file = stdin.lock();
            let (tmp_file_size, tmp_file_hash, tmp_chunks) = unique_compressed_chunks(
                &mut src_file,
                &mut chunker,
                hasher,
                chunk_compressor,
                &pool,
                true,
                process_chunk,
            )
            .chain_err(|| "unable to compress chunk")?;
            total_file_size = tmp_file_size;
            file_hash = tmp_file_hash;
            chunk_order = tmp_chunks;
        } else {
            bail!("Missing input file")
        }
    }
    pool.join();

    println!(
        "Total chunks: {}, unique: {}, size: {}, avg chunk size: {}, compressed into: {}",
        chunk_order.len(),
        total_unique_chunks,
        size_to_str(total_unique_chunk_size),
        size_to_str(total_unique_chunk_size / total_unique_chunks),
        size_to_str(total_compressed_size)
    );

    Ok(ChunkFileDescriptor {
        total_file_size,
        file_hash,
        chunk_descriptors,
        chunk_order,
    })
}

pub fn run(config: &CompressConfig, pool: &ThreadPool) -> Result<()> {
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(config.base.force_create)
        .truncate(config.base.force_create)
        .create_new(!config.base.force_create)
        .open(&config.output)
        .chain_err(|| format!("unable to create output file ({})", config.output.display()))?;

    std::fs::create_dir_all(&config.chunk_store_path)
        .chain_err(|| "Failed to create directory for chunk store")?;

    let mut tmp_chunk_file = OpenOptions::new()
        .write(true)
        .read(true)
        .truncate(true)
        .create(true)
        .open(&config.temp_file)
        .chain_err(|| "unable to create temporary chunk file")?;

    // Generate chunks and store to a temp file
    let chunk_file_descriptor = chunk_into_file(&config, pool, &mut tmp_chunk_file)?;

    println!(
        "Source hash: {}",
        HexSlice::new(&chunk_file_descriptor.file_hash)
    );

    // Store header to output file
    let file_header = chunk_dictionary::ChunkDictionary {
        rebuild_order: chunk_file_descriptor
            .chunk_order
            .iter()
            .map(|source_descriptor| source_descriptor.unique_chunk_index as u32)
            .collect(),
        application_version: ::PKG_VERSION.to_string(),
        chunk_descriptors: RepeatedField::from_vec(chunk_file_descriptor.chunk_descriptors),
        source_checksum: chunk_file_descriptor.file_hash,
        chunk_stores: RepeatedField::from_vec(vec![chunk_dictionary::ChunkStore {
            store_type: chunk_dictionary::ChunkStore_StoreType::CHUNK_FILE,
            store_path: config
                .chunk_store_path
                .to_str()
                .chain_err(|| "couldn't stringify chunk store path")?
                .to_string(),
            unknown_fields: std::default::Default::default(),
            cached_size: std::default::Default::default(),
        }]),
        source_total_size: chunk_file_descriptor.total_file_size as u64,
        chunker_params: SingularPtrField::some(chunk_dictionary::ChunkerParameters {
            chunk_filter_bits: config.chunk_filter_bits,
            min_chunk_size: config.min_chunk_size as u64,
            max_chunk_size: config.max_chunk_size as u64,
            hash_window_size: config.hash_window_size as u32,
            chunk_hash_length: config.hash_length as u32,
            unknown_fields: std::default::Default::default(),
            cached_size: std::default::Default::default(),
        }),
        unknown_fields: std::default::Default::default(),
        cached_size: std::default::Default::default(),
    };

    // Copy chunks from temporary chunk tile to the output one
    let header_buf = archive::build_header(&file_header)?;
    println!("Header size: {}", header_buf.len());
    output_file
        .write_all(&header_buf)
        .chain_err(|| "failed to write header")?;
    tmp_chunk_file
        .seek(SeekFrom::Start(0))
        .chain_err(|| "failed to seek")?;
    io::copy(&mut tmp_chunk_file, &mut output_file)
        .chain_err(|| "failed to write chunk data to output file")?;
    drop(tmp_chunk_file);
    fs::remove_file(&config.temp_file).chain_err(|| "unable to remove temporary file")?;

    Ok(())
}
