use blake2::{Blake2b, Digest};
use chunker_utils::HashBuf;
use lzma::LzmaWriter;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::prelude::*;
use std::path::Path;
use std::rc::Rc;
use string_utils::*;

use archive;
use chunk_dictionary;
use errors::*;

// Skip forward in a file/stream which is non seekable
fn skip_bytes<T>(input: &mut T, skip_bytes: u64, skip_buffer: &mut [u8]) -> Result<()>
where
    T: Read,
{
    let mut total_read = 0;
    while total_read < skip_bytes {
        let read_bytes = std::cmp::min(skip_buffer.len(), (skip_bytes - total_read) as usize);
        input
            .read_exact(&mut skip_buffer[0..read_bytes])
            .chain_err(|| "failed to skip")?;
        total_read += read_bytes as u64;
    }
    Ok(())
}

#[derive(Debug)]
pub struct ArchiveReader {
    // Go from chunk hash to archive chunk index (chunks vector)
    chunk_map: HashMap<HashBuf, usize>,

    // Array of chunk descriptors
    chunk_descriptors: Vec<archive::ChunkDescriptor>,

    // Go from archive chunk index to array of source offsets
    chunk_offsets: Vec<Vec<u64>>,

    // The order of chunks in source
    rebuild_order: Vec<usize>,

    pub chunk_stores: Vec<Rc<archive::ChunkStore>>,

    pub created_by_app_version: String,

    // Size of the original source file
    pub source_total_size: u64,
    pub source_checksum: HashBuf,

    // Chunker parameters used when this archive was created
    pub chunk_filter_bits: u32,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
    pub hash_window_size: usize,
    pub hash_length: usize,
}

impl fmt::Display for ArchiveReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "build version: {}, chunks: {} (unique: {}), chunk stores: '{:?}', decompressed size: {}, source checksum: {}",
            self.created_by_app_version,
            self.rebuild_order.len(),
            self.chunk_descriptors.len(),
            self.chunk_stores,
            size_to_str(self.source_total_size),
            HexSlice::new(&self.source_checksum),
        )
    }
}

// Trait to implement for archive backends.
pub trait ArchiveBackend
where
    Self: Read,
{
    // Read from archive into the given buffer.
    // Should read the exact number of bytes of the given buffer and start read at
    // given offset.
    fn read_at(&mut self, store_path: &str, offset: u64, buf: &mut [u8]) -> Result<()>;

    // Read and return chunked data
    fn read_in_chunks<F: FnMut(Vec<u8>) -> Result<()>>(
        &mut self,
        store_path: &str,
        start_offset: u64,
        chunk_sizes: &[u64],
        chunk_callback: F,
    ) -> Result<()>;
}

impl ArchiveReader {
    pub fn verify_pre_header(pre_header: &[u8]) -> Result<()> {
        if pre_header.len() < 5 {
            bail!("failed to read header of archive")
        }
        if &pre_header[0..4] != b"bita" {
            return Err(Error::from_kind(ErrorKind::NotAnArchive(
                "missing magic".to_string(),
            )));
        }
        if pre_header[4] != 0 {
            return Err(Error::from_kind(ErrorKind::NotAnArchive(
                "unknown archive version".to_string(),
            )));
        }
        Ok(())
    }

    pub fn try_init<R>(input: &mut R, header_buf: &mut Vec<u8>) -> Result<Self>
    where
        R: Read,
    {
        // Read the pre-header (file magic, version and size)
        header_buf.resize(13, 0);
        input
            .read_exact(header_buf)
            .or_else(|err| {
                header_buf.clear();
                Err(err)
            })
            .chain_err(|| "unable to read archive")?;

        Self::verify_pre_header(&header_buf[0..13])?;

        let dictionary_size = archive::vec_to_size(&header_buf[5..13]) as usize;

        // Read the dictionary, chunk data offset and header hash
        header_buf.resize(13 + dictionary_size + 64, 0);
        input
            .read_exact(&mut header_buf[13..])
            .chain_err(|| "unable to read archive")?;

        // Verify the header against the header hash
        let mut hasher = Blake2b::new();
        let offs = 13 + dictionary_size;
        hasher.input(&header_buf[..offs]);
        if header_buf[offs..(offs + 64)] != hasher.result().to_vec()[..] {
            return Err(Error::from_kind(ErrorKind::NotAnArchive(
                "corrupt archive header".to_string(),
            )));
        }

        // Deserialize the chunk dictionary
        let offs = 13;
        let dictionary: chunk_dictionary::ChunkDictionary =
            protobuf::parse_from_bytes(&header_buf[offs..(offs + dictionary_size)])
                .chain_err(|| "unable to unpack archive header")?;

        // Extract and store parameters from file header
        let chunk_stores: Vec<Rc<archive::ChunkStore>> = dictionary
            .chunk_stores
            .into_vec()
            .iter()
            .map(|cs| {
                let path = Path::new(&cs.store_path).to_path_buf();
                Rc::new(match cs.store_type {
                    chunk_dictionary::ChunkStore_StoreType::CHUNK_DIRECTORY => {
                        archive::ChunkStore::Directory(path)
                    }
                    chunk_dictionary::ChunkStore_StoreType::CHUNK_ARCHIVE => {
                        archive::ChunkStore::Archive(path)
                    }
                })
            })
            .collect();

        let source_total_size = dictionary.source_total_size;
        let source_checksum = dictionary.source_checksum;
        let created_by_app_version = dictionary.application_version;
        let chunker_params = dictionary.chunker_params.unwrap();

        let mut chunk_descriptors: Vec<archive::ChunkDescriptor> = Vec::new();
        let mut chunk_map: HashMap<HashBuf, usize> = HashMap::new();

        // Create map to go from chunk hash to descriptor index
        for (index, desc) in dictionary.chunk_descriptors.iter().enumerate() {
            chunk_map.insert(desc.checksum.clone(), index);
            chunk_descriptors.push(archive::ChunkDescriptor {
                checksum: desc.checksum.clone(),
                size: desc.size,
                compression: desc.compression.clone().unwrap_or_default(),
                stored_size: desc.stored_size,
                store_offset: desc.store_offset,
                // Use Rc to only store a reference to the actual store object
                store: Rc::clone(&chunk_stores[desc.store_index as usize]),
            });
        }

        // Create chunk offset vector, to go from chunk index to source file offsets
        let mut chunk_offsets = vec![vec![]; chunk_descriptors.len()];
        {
            let mut current_offset = 0;
            for descriptor_index in dictionary.rebuild_order.iter() {
                let descriptor_index = *descriptor_index as usize;
                let chunk_size = chunk_descriptors[descriptor_index].size;
                chunk_offsets[descriptor_index].push(current_offset);
                current_offset += chunk_size;
            }
        }

        Ok(ArchiveReader {
            chunk_map,
            chunk_descriptors,
            chunk_offsets,
            source_total_size,
            source_checksum,
            created_by_app_version,
            chunk_stores,
            rebuild_order: dictionary
                .rebuild_order
                .into_iter()
                .map(|s| s as usize)
                .collect(),
            chunk_filter_bits: chunker_params.chunk_filter_bits,
            min_chunk_size: chunker_params.min_chunk_size as usize,
            max_chunk_size: chunker_params.max_chunk_size as usize,
            hash_window_size: chunker_params.hash_window_size as usize,
            hash_length: chunker_params.chunk_hash_length as usize,
        })
    }

    // Get a set of all chunks present in archive
    pub fn chunk_hash_set(&self) -> HashSet<HashBuf> {
        self.chunk_map.iter().map(|x| x.0.clone()).collect()
    }

    // Get source offsets of a chunk
    pub fn chunk_source_offsets(&self, hash: &[u8]) -> Vec<u64> {
        if let Some(index) = self.chunk_map.get(hash) {
            self.chunk_offsets[*index].clone()
        } else {
            vec![]
        }
    }

    // Decompress chunk data, if compressed
    fn decompress_chunk(
        compression: chunk_dictionary::ChunkCompression_CompressionType,
        archive_data: Vec<u8>,
        chunk_data: &mut Vec<u8>,
    ) -> Result<()> {
        match compression {
            chunk_dictionary::ChunkCompression_CompressionType::LZMA => {
                // Archived chunk is compressed with lzma
                chunk_data.clear();
                let mut f =
                    LzmaWriter::new_decompressor(chunk_data).expect("new lzma decompressor");
                f.write_all(&archive_data).expect("write lzma decompressor");
                f.finish().expect("finish lzma decompressor");
            }
            chunk_dictionary::ChunkCompression_CompressionType::ZSTD => {
                // Archived chunk is compressed with zstd
                chunk_data.clear();
                zstd::stream::copy_decode(&archive_data[..], chunk_data).expect("zstd decompress");
            }
            chunk_dictionary::ChunkCompression_CompressionType::NONE => {
                // Archived chunk is NOT compressed
                *chunk_data = archive_data;
            }
        }

        Ok(())
    }

    // Group chunks which are placed in sequence inside a chunk archive
    fn group_chunks_in_sequence(
        mut chunks: Vec<&archive::ChunkDescriptor>,
    ) -> Vec<Vec<&archive::ChunkDescriptor>> {
        let mut group_list = vec![];

        if chunks.is_empty() {
            return group_list;
        }

        let mut group = vec![chunks.remove(0)];
        while !chunks.is_empty() {
            let chunk = chunks.remove(0);

            let prev_chunk_end;
            let prev_chunk_store;
            {
                let prev_chunk = group.last().unwrap();
                prev_chunk_end = prev_chunk.store_offset + prev_chunk.stored_size;
                prev_chunk_store = &prev_chunk.store;
            }

            if chunk.store == *prev_chunk_store && prev_chunk_end == chunk.store_offset {
                // Chunk lives in the same store as the previous and is placed right
                // next to the previous chunk. Will only be true for chunk data
                // inside an archive. In a directory store the chunk offset will always
                // be 0 hence this comparison should not be true.
                group.push(chunk);
            } else {
                group_list.push(group);
                group = vec![chunk];
            }
        }
        group_list.push(group);
        group_list
    }

    // Get chunk data for all listed chunks if present in archive
    pub fn read_chunk_data<T, F>(
        &self,
        mut input: T,
        chunks: &HashSet<HashBuf>,
        mut result: F,
    ) -> Result<u64>
    where
        T: ArchiveBackend,
        F: FnMut(&archive::ChunkDescriptor, &[u8]) -> Result<()>,
    {
        // Create list of chunks which are in archive. The order of the list should
        // be the same otder as the chunk data in archive.
        let descriptors: Vec<&archive::ChunkDescriptor> = self
            .chunk_descriptors
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum))
            .collect();

        // Create groups of chunks so that we can make a single request for all chunks
        // which are placed in sequence in archive.
        let grouped_chunks = Self::group_chunks_in_sequence(descriptors);

        let mut hasher = Blake2b::new();
        let mut total_read = 0;
        let mut chunk_data = vec![];

        for group in grouped_chunks {
            // For each group of chunks
            let start_offset = group[0].store_offset;
            let group_path = group[0]
                .store_path()
                .to_str()
                .chain_err(|| "failed to get string from path")?
                .to_string();
            let chunk_sizes: Vec<u64> = group.iter().map(|c| c.stored_size).collect();
            let mut chunk_index = 0;

            input
                .read_in_chunks(&group_path, start_offset, &chunk_sizes, |archive_data| {
                    // For each chunk read from archive
                    let chunk_descriptor = &group[chunk_index];
                    Self::decompress_chunk(
                        chunk_descriptor.compression.get_compression(),
                        archive_data,
                        &mut chunk_data,
                    )?;

                    total_read += chunk_descriptor.stored_size;

                    // Verify data by hash
                    hasher.input(&chunk_data);
                    let checksum = hasher.result_reset().to_vec();
                    if checksum[..self.hash_length] != chunk_descriptor.checksum[..self.hash_length]
                    {
                        bail!(
                            "Chunk hash mismatch (expected: {}, got: {})",
                            HexSlice::new(&chunk_descriptor.checksum[0..self.hash_length]),
                            HexSlice::new(&checksum[0..self.hash_length])
                        );
                    }

                    // For each offset where this chunk was found in source
                    result(chunk_descriptor, &chunk_data)?;

                    chunk_index += 1;

                    Ok(())
                })
                .expect("read chunks");
        }

        Ok(total_read)
    }

    // Get chunk data for all listed chunks if present in archive
    pub fn read_chunk_stream<T, F>(
        &mut self,
        mut input: T,
        mut current_input_offset: u64,
        chunks: &HashSet<HashBuf>,
        mut result: F,
    ) -> Result<u64>
    where
        T: Read,
        F: FnMut(&archive::ChunkDescriptor, &[u8]) -> Result<()>,
    {
        let mut total_read = 0;
        let mut hasher = Blake2b::new();
        let mut skip_buffer = vec![0; 1024 * 1024];
        let mut chunk_data = vec![];

        // Sort list of chunks to be read in order of occurence in stream.
        let descriptors = self
            .chunk_descriptors
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum));

        for chunk_descriptor in descriptors {
            // Read through the stream and pick the chunk data requsted.

            let mut archive_data = vec![0; chunk_descriptor.stored_size as usize];
            let abs_chunk_offset = chunk_descriptor.store_offset;

            // Skip until chunk
            let skip = abs_chunk_offset - current_input_offset;
            skip_bytes(&mut input, skip, &mut skip_buffer)?;

            // Read chunk data
            input
                .read_exact(&mut archive_data)
                .chain_err(|| "failed to read from archive")?;

            Self::decompress_chunk(
                chunk_descriptor.compression.get_compression(),
                archive_data,
                &mut chunk_data,
            )?;

            // Verify data by hash
            hasher.input(&chunk_data);
            let checksum = hasher.result_reset().to_vec();
            if checksum[..self.hash_length] != chunk_descriptor.checksum[..self.hash_length] {
                bail!(
                    "Chunk hash mismatch (expected: {}, got: {})",
                    HexSlice::new(&chunk_descriptor.checksum[0..self.hash_length]),
                    HexSlice::new(&checksum[0..self.hash_length])
                );
            }

            current_input_offset += skip;
            current_input_offset += chunk_descriptor.stored_size;
            total_read += chunk_descriptor.stored_size;

            // For each offset where this chunk was found in source
            result(chunk_descriptor, &chunk_data)?;
        }
        Ok(total_read)
    }
}
