use blake2::{Blake2b, Digest};
use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use archive_reader::*;
use chunk_dictionary;
use chunker::Chunker;
use chunker_utils::*;
use errors::*;
use local_reader_backend::LocalReaderBackend;
use threadpool::ThreadPool;

#[derive(PartialEq)]
pub enum DataVerified {
    Yes,
    No,
}

pub fn from_stream<T, F>(
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
        DataVerified,
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
                    DataVerified::Yes,
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

pub fn from_file<F>(
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
        DataVerified,
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

            from_stream(
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
                        DataVerified::No,
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
