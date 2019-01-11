use blake2::{Blake2b, Digest};
use lzma::LzmaWriter;
use protobuf::Message;
use std::fmt;
use std::io::prelude::*;
use std::path::PathBuf;
use std::rc::Rc;

use crate::chunk_dictionary;
use crate::chunker_utils::HashBuf;
use crate::errors::*;
use crate::string_utils::*;

impl fmt::Display for chunk_dictionary::ChunkDictionary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "version: {}, chunks: {}, source hash: {}, source size: {}, chunk stores: '{:?}'",
            self.application_version,
            self.chunk_descriptors.len(),
            HexSlice::new(&self.source_checksum),
            size_to_str(self.source_total_size),
            self.chunk_stores
        )
    }
}

fn size_vec(s: u64) -> [u8; 8] {
    [
        ((s >> 56) & 0xff) as u8,
        ((s >> 48) & 0xff) as u8,
        ((s >> 40) & 0xff) as u8,
        ((s >> 32) & 0xff) as u8,
        ((s >> 24) & 0xff) as u8,
        ((s >> 16) & 0xff) as u8,
        ((s >> 8) & 0xff) as u8,
        (s & 0xff) as u8,
    ]
}

pub fn vec_to_size(sv: &[u8]) -> u64 {
    (u64::from(sv[0]) << 56)
        | (u64::from(sv[1]) << 48)
        | (u64::from(sv[2]) << 40)
        | (u64::from(sv[3]) << 32)
        | (u64::from(sv[4]) << 24)
        | (u64::from(sv[5]) << 16)
        | (u64::from(sv[6]) << 8)
        | u64::from(sv[7])
}

#[derive(Debug, PartialEq)]
pub enum ChunkStore {
    Directory(PathBuf),
    Archive(PathBuf),
}

#[derive(Debug)]
pub struct ChunkDescriptor {
    pub checksum: HashBuf,
    pub size: u64,
    pub compression: chunk_dictionary::ChunkCompression,
    pub stored_size: u64,
    pub store_offset: u64,
    pub store: Rc<ChunkStore>,
}

impl ChunkDescriptor {
    pub fn store_path(&self) -> PathBuf {
        match *self.store {
            ChunkStore::Directory(ref store_path) => store_path
                .join(buf_to_hex_str(&self.checksum))
                .with_extension(
                    "chunk".to_string()
                        + compression_type_file_ending(self.compression.get_compression()),
                ),
            ChunkStore::Archive(ref store_path) => store_path.to_path_buf(),
        }
    }
}

impl fmt::Display for chunk_dictionary::ChunkCompression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.compression {
            chunk_dictionary::ChunkCompression_CompressionType::NONE => write!(f, "NONE"),
            chunk_dictionary::ChunkCompression_CompressionType::LZMA => {
                write!(f, "LZMA({})", self.compression_level)
            }
            chunk_dictionary::ChunkCompression_CompressionType::ZSTD => {
                write!(f, "ZSTD({})", self.compression_level)
            }
        }
    }
}

pub fn compression_type_file_ending(
    compression_type: chunk_dictionary::ChunkCompression_CompressionType,
) -> &'static str {
    match compression_type {
        chunk_dictionary::ChunkCompression_CompressionType::LZMA => ".lzma",
        chunk_dictionary::ChunkCompression_CompressionType::ZSTD => ".zst",
        chunk_dictionary::ChunkCompression_CompressionType::NONE => "",
    }
}

// Decompress chunk data, if compressed
pub fn decompress_chunk(
    compression: chunk_dictionary::ChunkCompression_CompressionType,
    archive_data: Vec<u8>,
    chunk_data: &mut Vec<u8>,
) -> Result<()> {
    match compression {
        chunk_dictionary::ChunkCompression_CompressionType::LZMA => {
            // Archived chunk is compressed with lzma
            chunk_data.clear();
            let mut f = LzmaWriter::new_decompressor(chunk_data).expect("new lzma decompressor");
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

pub fn build_header(dictionary: &chunk_dictionary::ChunkDictionary) -> Result<Vec<u8>> {
    let mut header: Vec<u8> = vec![];
    let mut hasher = Blake2b::new();
    let mut dictionary_buf: Vec<u8> = Vec::new();

    dictionary
        .write_to_vec(&mut dictionary_buf)
        .chain_err(|| "failed to serialize header")?;

    // header magic
    header.extend(b"bita");

    // Major archive version
    header.push(0);

    // Chunk dictionary size
    header.extend(&size_vec(dictionary_buf.len() as u64));

    // The chunk dictionary
    header.extend(dictionary_buf);

    // Create and set hash of full header
    hasher.input(&header);
    let hash = hasher.result().to_vec();
    println!("Dictionary hash: {}", HexSlice::new(&hash));
    header.extend(hash);

    Ok(header)
}
