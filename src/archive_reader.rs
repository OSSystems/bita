use blake2::{Blake2b, Digest};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::prelude::*;
use std::path::Path;
use std::rc::Rc;

use crate::archive;
use crate::chunk_dictionary;
use crate::chunker_utils::HashBuf;
use crate::errors::*;
use crate::string_utils::*;

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
    fn read_at(&mut self, store_path: &Path, offset: u64, buf: &mut [u8]) -> Result<()>;

    // Read and return chunked data
    fn read_in_chunks<F: FnMut(Vec<u8>) -> Result<()>>(
        &mut self,
        store_path: &Path,
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
                compression: desc.compression.clone().unwrap_or_default().into(),
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
        mut chunk_data_callback: F,
    ) -> Result<u64>
    where
        T: ArchiveBackend,
        F: FnMut(&archive::ChunkDescriptor, Vec<u8>) -> Result<()>,
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

        let mut total_read = 0;

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
                .read_in_chunks(
                    &Path::new(&group_path),
                    start_offset,
                    &chunk_sizes,
                    |archive_data| {
                        // For each offset where this chunk was found in source
                        let chunk_descriptor = &group[chunk_index];
                        chunk_data_callback(chunk_descriptor, archive_data)?;

                        total_read += chunk_descriptor.stored_size;
                        chunk_index += 1;

                        Ok(())
                    },
                )
                .expect("read chunks");
        }

        Ok(total_read)
    }
}
