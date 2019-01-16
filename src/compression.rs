use lzma::LzmaWriter;
use std::io::Write;

use crate::chunk_dictionary::{ChunkCompression, ChunkCompression_CompressionType};
use crate::errors::*;

#[derive(Debug, Clone, Copy)]
pub enum Compression {
    None,
    LZMA(u32),
    ZSTD(u32),
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Compression::LZMA(ref level) => write!(f, "LZMA({})", level),
            Compression::ZSTD(ref level) => write!(f, "ZSTD({})", level),
            Compression::None => write!(f, "None)"),
        }
    }
}

impl From<ChunkCompression> for Compression {
    fn from(compression: ChunkCompression) -> Self {
        match compression.compression {
            ChunkCompression_CompressionType::LZMA => {
                Compression::LZMA(compression.compression_level)
            }
            ChunkCompression_CompressionType::ZSTD => {
                Compression::ZSTD(compression.compression_level)
            }
            ChunkCompression_CompressionType::NONE => Compression::None,
        }
    }
}

impl From<Compression> for ChunkCompression {
    fn from(compression: Compression) -> Self {
        let (chunk_compression, chunk_compression_level) = match compression {
            Compression::LZMA(ref level) => (ChunkCompression_CompressionType::LZMA, *level),
            Compression::ZSTD(ref level) => (ChunkCompression_CompressionType::ZSTD, *level),
            Compression::None => (ChunkCompression_CompressionType::NONE, 0),
        };
        ChunkCompression {
            compression: chunk_compression,
            compression_level: chunk_compression_level,
            unknown_fields: std::default::Default::default(),
            cached_size: std::default::Default::default(),
        }
    }
}

impl Compression {
    // Get compression type file extension
    pub fn file_extension(&self) -> &'static str {
        match self {
            Compression::LZMA(_) => ".lzma",
            Compression::ZSTD(_) => ".zst",
            Compression::None => "",
        }
    }

    // Compress a block of data with set compression
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            Compression::LZMA(ref level) => {
                let mut result = vec![];
                {
                    let mut f = LzmaWriter::new_compressor(&mut result, *level)
                        .chain_err(|| "failed to create lzma compressor")?;
                    f.write_all(data)
                        .chain_err(|| "failed compress with lzma")?;
                    f.finish()
                        .chain_err(|| "failed to finish lzma compressor")?;
                }
                Ok(result)
            }
            Compression::ZSTD(ref level) => {
                let mut result = vec![];
                let mut data = data.to_vec();
                zstd::stream::copy_encode(&data[..], &mut result, *level as i32)
                    .chain_err(|| "failed compress with zstd")?;
                Ok(result)
            }
            Compression::None => Ok(data.to_vec()),
        }
    }
}
