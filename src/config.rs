use std::path::PathBuf;

use crate::compression::Compression;

#[derive(Debug)]
pub struct BaseConfig {
    pub force_create: bool,
}

#[derive(Debug)]
pub enum ChunkStoreType {
    Directory(PathBuf),
    Archive(PathBuf),
}

#[derive(Debug)]
pub struct CompressConfig {
    pub base: BaseConfig,

    // Use stdin if input not given
    pub input: Option<PathBuf>,
    pub output: PathBuf,
    pub chunk_store: ChunkStoreType,
    pub hash_length: usize,
    pub chunk_filter_bits: u32,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
    pub hash_window_size: usize,
    pub compression: Compression,
}

#[derive(Debug, Clone)]
pub enum CloneTarget {
    StoreDirectory(Compression),
    StoreArchive(Compression),
    Unpack,
}

#[derive(Debug)]
pub struct CloneConfig {
    pub base: BaseConfig,

    pub input: String,
    pub output_path: PathBuf,
    pub seed_files: Vec<PathBuf>,
    pub target: CloneTarget,
}

#[derive(Debug)]
pub enum Config {
    Compress(CompressConfig),
    Clone(CloneConfig),
}
