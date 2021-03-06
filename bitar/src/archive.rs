use blake2::{Blake2b, Digest};

use crate::{
    chunk_dictionary as dict,
    chunk_dictionary::{
        chunk_compression::CompressionType, chunker_parameters::ChunkingAlgorithm,
        ChunkCompression, ChunkerParameters,
    },
    chunker, header, ChunkIndex, Compression, CompressionError, HashSum, Reader,
};

#[derive(Debug)]
pub enum ArchiveError<R> {
    NotAnArchive,
    InvalidHeaderChecksum,
    CorruptArchive,
    ReaderError(R),
    UnknownChunkingAlgorithm,
    DictionaryDecode(prost::DecodeError),
    UnknownCompression,
    CompressionError(CompressionError),
}
impl<R> std::error::Error for ArchiveError<R> where R: std::error::Error {}
impl<R> std::fmt::Display for ArchiveError<R>
where
    R: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAnArchive => write!(f, "not an archive"),
            Self::InvalidHeaderChecksum => write!(f, "invalid archive header"),
            Self::CorruptArchive => write!(f, "corrupt archive"),
            Self::ReaderError(err) => write!(f, "reader error: {}", err),
            Self::UnknownChunkingAlgorithm => write!(f, "unknown chunking algorithm"),
            Self::DictionaryDecode(err) => write!(f, "dictionary decode error: {}", err),
            Self::UnknownCompression => write!(f, "unknown chunk compression"),
            Self::CompressionError(err) => write!(f, "compression error: {}", err),
        }
    }
}
impl<E> From<prost::DecodeError> for ArchiveError<E> {
    fn from(err: prost::DecodeError) -> Self {
        Self::DictionaryDecode(err)
    }
}
impl<E> From<CompressionError> for ArchiveError<E> {
    fn from(err: CompressionError) -> Self {
        Self::CompressionError(err)
    }
}

/// Description of a chunk within an archive.
#[derive(Clone, Debug, PartialEq)]
pub struct ChunkDescriptor {
    /// Chunk checksum.
    pub checksum: HashSum,
    /// Actual size of chunk data in the archive (may be compressed).
    pub archive_size: u32,
    /// Byte offset of chunk in the archive.
    pub archive_offset: u64,
    /// Size of the chunk data in source (uncompressed).
    pub source_size: u32,
}

impl From<dict::ChunkDescriptor> for ChunkDescriptor {
    fn from(dict: dict::ChunkDescriptor) -> Self {
        ChunkDescriptor {
            checksum: dict.checksum.into(),
            archive_size: dict.archive_size,
            archive_offset: dict.archive_offset,
            source_size: dict.source_size,
        }
    }
}

/// A readable archive.
pub struct Archive {
    // Array with descriptor of all chunks in archive.
    archive_chunks: Vec<ChunkDescriptor>,
    // Array of indexes pointing into the archive_chunks.
    // Represents the order of chunks in source.
    source_order: Vec<usize>,

    total_chunks: usize,
    header_size: usize,
    header_checksum: HashSum,
    chunk_compression: Compression,
    created_by_app_version: String,
    chunk_data_offset: u64,
    source_total_size: u64,
    source_checksum: HashSum,
    chunker_config: chunker::Config,
    chunk_hash_length: usize,
}

impl Archive {
    fn verify_pre_header<E>(pre_header: &[u8]) -> Result<(), ArchiveError<E>> {
        if pre_header.len() < header::ARCHIVE_MAGIC.len() {
            return Err(ArchiveError::NotAnArchive);
        }
        // Allow both legacy type file magic (prefixed with \0 but no null
        // termination) and 'BITA\0'.
        if &pre_header[0..header::ARCHIVE_MAGIC.len()] != header::ARCHIVE_MAGIC
            && &pre_header[0..header::ARCHIVE_MAGIC.len()] != b"\0BITA1"
        {
            return Err(ArchiveError::NotAnArchive);
        }
        Ok(())
    }
    /// Try to initialize an archive from a reader.
    pub async fn try_init<R>(reader: &mut R) -> Result<Self, ArchiveError<R::Error>>
    where
        R: Reader,
    {
        // Read the pre-header (file magic and size)
        let mut header: Vec<u8> = reader
            .read_at(0, header::PRE_HEADER_SIZE)
            .await
            .map_err(ArchiveError::ReaderError)?
            .to_vec();
        Self::verify_pre_header(&header)?;

        let dictionary_size =
            u64_from_le_slice(&header[header::ARCHIVE_MAGIC.len()..header::PRE_HEADER_SIZE])
                as usize;

        // Read the dictionary, chunk data offset and header hash
        header.extend_from_slice(
            &reader
                .read_at(header::PRE_HEADER_SIZE as u64, dictionary_size + 8 + 64)
                .await
                .map_err(ArchiveError::ReaderError)?,
        );

        // Verify the header against the header checksum
        let header_checksum = {
            let mut hasher = Blake2b::new();
            let offs = header::PRE_HEADER_SIZE + dictionary_size + 8;
            hasher.input(&header[..offs]);
            let header_checksum = HashSum::from_slice(&header[offs..(offs + 64)]);
            if header_checksum != &hasher.result()[..] {
                return Err(ArchiveError::InvalidHeaderChecksum);
            }
            header_checksum
        };

        // Deserialize the chunk dictionary
        let dictionary: dict::ChunkDictionary = {
            let offs = header::PRE_HEADER_SIZE;
            prost::Message::decode(&header[offs..(offs + dictionary_size)])?
        };

        // Get chunk data offset
        let chunk_data_offset = {
            let offs = header::PRE_HEADER_SIZE + dictionary_size;
            u64_from_le_slice(&header[offs..(offs + 8)])
        };
        let archive_chunks = dictionary
            .chunk_descriptors
            .into_iter()
            .map(ChunkDescriptor::from)
            .collect();
        let chunker_params = dictionary
            .chunker_params
            .ok_or(ArchiveError::CorruptArchive)?;
        let chunk_hash_length = chunker_params.chunk_hash_length as usize;
        let source_order: Vec<usize> = dictionary
            .rebuild_order
            .into_iter()
            .map(|v| v as usize)
            .collect();
        Ok(Self {
            archive_chunks,
            header_checksum,
            header_size: header.len(),
            source_total_size: dictionary.source_total_size,
            source_checksum: dictionary.source_checksum.into(),
            created_by_app_version: dictionary.application_version.clone(),
            chunk_compression: compression_from_dictionary(
                dictionary
                    .chunk_compression
                    .ok_or(ArchiveError::CorruptArchive)?,
            )?,
            total_chunks: source_order.len(),
            source_order,
            chunk_data_offset,
            chunk_hash_length,
            chunker_config: chunker_config_from_params(chunker_params)?,
        })
    }
    /// Total number of chunks in archive (including duplicates).
    pub fn total_chunks(&self) -> usize {
        self.total_chunks
    }
    /// Total number of unique chunks in archive (no duplicates).
    pub fn unique_chunks(&self) -> usize {
        self.archive_chunks.len()
    }
    /// Total size of chunks in archive when compressed.
    pub fn compressed_size(&self) -> u64 {
        self.archive_chunks
            .iter()
            .map(|c| u64::from(c.archive_size))
            .sum()
    }
    /// On which offset in the archive the chunk data starts at.
    pub fn chunk_data_offset(&self) -> u64 {
        self.chunk_data_offset
    }
    /// Get archive chunk descriptors.
    pub fn chunk_descriptors(&self) -> &[ChunkDescriptor] {
        &self.archive_chunks
    }
    /// Total size of the original source file.
    pub fn total_source_size(&self) -> u64 {
        self.source_total_size
    }
    /// Checksum of the original source file (Blake2).
    pub fn source_checksum(&self) -> &HashSum {
        &self.source_checksum
    }
    /// Get the chunker configuration used when building the archive.
    pub fn chunker_config(&self) -> &chunker::Config {
        &self.chunker_config
    }
    /// Get the checksum of the archive header.
    pub fn header_checksum(&self) -> &HashSum {
        &self.header_checksum
    }
    /// Get the size of the archive header.
    pub fn header_size(&self) -> usize {
        self.header_size
    }
    /// Get the hash length used for identifying chunks when building the archive.
    pub fn chunk_hash_length(&self) -> usize {
        self.chunk_hash_length
    }
    /// Get the compression used for chunks in the archive.
    pub fn chunk_compression(&self) -> Compression {
        self.chunk_compression
    }
    /// Get the version of crate used when building the archive.
    pub fn built_with_version(&self) -> &str {
        &self.created_by_app_version
    }
    /// Iterate chunks as ordered in source.
    pub fn iter_source_chunks(&self) -> impl Iterator<Item = (u64, &ChunkDescriptor)> {
        let mut chunk_offset = 0;
        self.source_order.iter().copied().map(move |index| {
            let offset = chunk_offset;
            let cd = &self.archive_chunks[index as usize];
            chunk_offset += cd.source_size as u64;
            (offset, cd)
        })
    }
    /// Build a ChunkIndex representing the source file.
    pub fn build_source_index(&self) -> ChunkIndex {
        let mut ci = ChunkIndex::new_empty();
        self.iter_source_chunks().for_each(|(offset, cd)| {
            ci.add_chunk(cd.checksum.clone(), cd.source_size as usize, &[offset]);
        });
        ci
    }
    /// Returns chunks at adjacent location in archive grouped together.
    pub fn grouped_chunks(&self, filter_chunks: &ChunkIndex) -> Vec<Vec<ChunkDescriptor>> {
        group_chunks(
            self.archive_chunks
                .iter()
                .filter(|chunk| filter_chunks.contains(&chunk.checksum)),
        )
    }
}

fn group_chunks<'a>(
    chunk_order: impl Iterator<Item = &'a ChunkDescriptor>,
) -> Vec<Vec<ChunkDescriptor>> {
    let mut group_list = Vec::new();
    let mut group: Vec<ChunkDescriptor> = Vec::new();
    for descriptor in chunk_order.cloned() {
        if let Some(prev) = group.last() {
            let prev_chunk_end = prev.archive_offset + u64::from(prev.archive_size);
            if prev_chunk_end == descriptor.archive_offset {
                // Chunk is placed right next to the previous chunk
                group.push(descriptor);
            } else {
                group_list.push(group);
                group = vec![descriptor];
            }
        } else {
            group.push(descriptor);
        }
    }
    if !group.is_empty() {
        group_list.push(group);
    }
    group_list
}

fn chunker_config_from_params<R>(p: ChunkerParameters) -> Result<chunker::Config, ArchiveError<R>> {
    match ChunkingAlgorithm::from_i32(p.chunking_algorithm) {
        Some(ChunkingAlgorithm::Buzhash) => Ok(chunker::Config::BuzHash(chunker::FilterConfig {
            filter_bits: chunker::FilterBits::from_bits(p.chunk_filter_bits),
            min_chunk_size: p.min_chunk_size as usize,
            max_chunk_size: p.max_chunk_size as usize,
            window_size: p.rolling_hash_window_size as usize,
        })),
        Some(ChunkingAlgorithm::Rollsum) => Ok(chunker::Config::RollSum(chunker::FilterConfig {
            filter_bits: chunker::FilterBits::from_bits(p.chunk_filter_bits),
            min_chunk_size: p.min_chunk_size as usize,
            max_chunk_size: p.max_chunk_size as usize,
            window_size: p.rolling_hash_window_size as usize,
        })),
        Some(ChunkingAlgorithm::FixedSize) => {
            Ok(chunker::Config::FixedSize(p.max_chunk_size as usize))
        }
        _ => Err(ArchiveError::UnknownChunkingAlgorithm),
    }
}

fn compression_from_dictionary<R>(c: ChunkCompression) -> Result<Compression, ArchiveError<R>> {
    match CompressionType::from_i32(c.compression) {
        #[cfg(feature = "lzma-compression")]
        Some(CompressionType::Lzma) => Ok(Compression::LZMA(c.compression_level)),
        #[cfg(not(feature = "lzma-compression"))]
        Some(CompressionType::Lzma) => panic!("LZMA compression not enabled"),
        #[cfg(feature = "zstd-compression")]
        Some(CompressionType::Zstd) => Ok(Compression::ZSTD(c.compression_level)),
        #[cfg(not(feature = "zstd-compression"))]
        Some(CompressionType::Zstd) => panic!("ZSTD compression not enabled"),
        Some(CompressionType::Brotli) => Ok(Compression::Brotli(c.compression_level)),
        Some(CompressionType::None) => Ok(Compression::None),
        None => Err(ArchiveError::UnknownCompression),
    }
}

fn u64_from_le_slice(v: &[u8]) -> u64 {
    let mut tmp: [u8; 8] = Default::default();
    tmp.copy_from_slice(v);
    u64::from_le_bytes(tmp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouped_chunks_all_adjacent() {
        let chunk_order = [
            ChunkDescriptor {
                checksum: vec![0].into(),
                archive_offset: 0,
                archive_size: 10,
                source_size: 10,
            },
            ChunkDescriptor {
                checksum: vec![1].into(),
                archive_offset: 10,
                archive_size: 10,
                source_size: 10,
            },
        ];
        assert_eq!(
            group_chunks(chunk_order.iter()),
            vec![vec![
                ChunkDescriptor {
                    checksum: vec![0].into(),
                    archive_offset: 0,
                    archive_size: 10,
                    source_size: 10,
                },
                ChunkDescriptor {
                    checksum: vec![1].into(),
                    archive_offset: 10,
                    archive_size: 10,
                    source_size: 10,
                },
            ]]
        );
    }
    #[test]
    fn grouped_chunks_no_adjacent() {
        let chunk_order = [
            ChunkDescriptor {
                checksum: vec![0].into(),
                archive_offset: 0,
                archive_size: 10,
                source_size: 10,
            },
            ChunkDescriptor {
                checksum: vec![1].into(),
                archive_offset: 11,
                archive_size: 10,
                source_size: 10,
            },
        ];
        assert_eq!(
            group_chunks(chunk_order.iter()),
            vec![
                vec![ChunkDescriptor {
                    checksum: vec![0].into(),
                    archive_offset: 0,
                    archive_size: 10,
                    source_size: 10,
                }],
                vec![ChunkDescriptor {
                    checksum: vec![1].into(),
                    archive_offset: 11,
                    archive_size: 10,
                    source_size: 10,
                },]
            ]
        );
    }
    #[test]
    fn grouped_chunks_first_adjacent() {
        let chunk_order = [
            ChunkDescriptor {
                checksum: vec![0].into(),
                archive_offset: 0,
                archive_size: 10,
                source_size: 10,
            },
            ChunkDescriptor {
                checksum: vec![1].into(),
                archive_offset: 10,
                archive_size: 10,
                source_size: 10,
            },
            ChunkDescriptor {
                checksum: vec![2].into(),
                archive_offset: 21,
                archive_size: 10,
                source_size: 10,
            },
        ];
        assert_eq!(
            group_chunks(chunk_order.iter()),
            vec![
                vec![
                    ChunkDescriptor {
                        checksum: vec![0].into(),
                        archive_offset: 0,
                        archive_size: 10,
                        source_size: 10,
                    },
                    ChunkDescriptor {
                        checksum: vec![1].into(),
                        archive_offset: 10,
                        archive_size: 10,
                        source_size: 10,
                    }
                ],
                vec![ChunkDescriptor {
                    checksum: vec![2].into(),
                    archive_offset: 21,
                    archive_size: 10,
                    source_size: 10,
                },]
            ]
        );
    }
    #[test]
    fn grouped_chunks_last_adjacent() {
        let chunk_order = [
            ChunkDescriptor {
                checksum: vec![0].into(),
                archive_offset: 0,
                archive_size: 10,
                source_size: 10,
            },
            ChunkDescriptor {
                checksum: vec![1].into(),
                archive_offset: 11,
                archive_size: 10,
                source_size: 10,
            },
            ChunkDescriptor {
                checksum: vec![2].into(),
                archive_offset: 21,
                archive_size: 10,
                source_size: 10,
            },
        ];
        assert_eq!(
            group_chunks(chunk_order.iter()),
            vec![
                vec![ChunkDescriptor {
                    checksum: vec![0].into(),
                    archive_offset: 0,
                    archive_size: 10,
                    source_size: 10,
                },],
                vec![
                    ChunkDescriptor {
                        checksum: vec![1].into(),
                        archive_offset: 11,
                        archive_size: 10,
                        source_size: 10,
                    },
                    ChunkDescriptor {
                        checksum: vec![2].into(),
                        archive_offset: 21,
                        archive_size: 10,
                        source_size: 10,
                    },
                ]
            ]
        );
    }
}
