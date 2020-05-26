#![recursion_limit = "256"]

mod archive;
mod chunk_index;
mod chunk_location;
mod chunk_location_map;
mod chunker;
mod compression;
mod hashsum;
mod header;
mod http_range_request;
mod reader;
mod reader_remote;
mod rolling_hash;

pub mod clone;

pub use archive::{Archive, ArchiveError};
pub use chunk_index::{ChunkIndex, ReorderOp};
pub use chunk_location::ChunkLocation;
pub use chunker::{Chunker, ChunkerConfig, ChunkerFilterBits, ChunkerFilterConfig};
pub use compression::{Compression, CompressionError};
pub use hashsum::HashSum;
pub use header::{build_header, ARCHIVE_MAGIC, PRE_HEADER_SIZE};
pub use reader::Reader;
pub use reader_remote::{ReaderRemote, ReaderRemoteError};
pub use rolling_hash::{BuzHash, RollSum, RollingHash};

pub mod chunk_dictionary {
    include!(concat!(env!("OUT_DIR"), "/chunk_dictionary.rs"));
}
