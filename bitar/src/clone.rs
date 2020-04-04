use async_trait::async_trait;
use futures_util::stream::StreamExt;
use futures_util::{future, pin_mut};
use log::*;
use num_cpus;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::{Archive, ChunkIndex, Chunker, ChunkerConfig, Error, HashSum, Reader, ReorderOp};

#[async_trait]
pub trait CloneOutput {
    /// Write a single chunk to output at the given offsets
    async fn write_chunk(
        &mut self,
        hash: &HashSum,
        offsets: &[u64],
        buf: &[u8],
    ) -> Result<(), Error>;
}

#[async_trait]
impl<T> CloneOutput for T
where
    T: AsyncWrite + AsyncSeek + Unpin + Send,
{
    async fn write_chunk(&mut self, _: &HashSum, offsets: &[u64], buf: &[u8]) -> Result<(), Error> {
        for &offset in offsets {
            self.seek(std::io::SeekFrom::Start(offset)).await?;
            self.write_all(buf).await?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait CloneInPlaceTarget: CloneOutput {
    /// Read a chunk identified by hash and/or offset
    async fn read_chunk(
        &mut self,
        hash: &HashSum,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<(), Error>;
    /// Generatea chunk index describing the target structure
    async fn chunk_index(
        &mut self,
        chunker_config: &ChunkerConfig,
        hash_length: usize,
    ) -> Result<ChunkIndex, Error>;
}

/// Clone options
#[derive(Default, Clone)]
pub struct CloneOptions {
    pub max_buffered_chunks: usize,
}

impl CloneOptions {
    /// Set the maximum number of chunk buffers to use while cloning
    ///
    /// 0 will result in an automatically selected value.
    pub fn max_buffered_chunks(mut self, num: usize) -> Self {
        self.max_buffered_chunks = num;
        self
    }
    fn get_max_buffers(&self) -> usize {
        if self.max_buffered_chunks == 0 {
            // Single buffer if we have a single core, otherwise number of cores x 2
            match num_cpus::get() {
                0 | 1 => 1,
                n => n * 2,
            }
        } else {
            self.max_buffered_chunks
        }
    }
}

/// Clone by moving data in output in-place
pub async fn clone_in_place(
    _opts: &CloneOptions,
    archive: &Archive,
    chunks: &mut ChunkIndex,
    target: &mut dyn CloneInPlaceTarget,
) -> Result<u64, Error> {
    let mut total_moved: u64 = 0;
    let target_index = target
        .chunk_index(archive.chunker_config(), archive.chunk_hash_length())
        .await?;
    let (already_in_place, in_place_total_size) =
        target_index.strip_chunks_already_in_place(chunks);
    debug!(
        "{} chunks ({}) are already in place in target",
        already_in_place, in_place_total_size
    );

    let reorder_ops = target_index.reorder_ops(chunks);
    let mut temp_store: HashMap<&HashSum, Vec<u8>> = HashMap::new();
    for op in &reorder_ops {
        // Move chunks around internally in the output file
        match op {
            ReorderOp::Copy { hash, source, dest } => {
                let buf = if let Some(buf) = temp_store.remove(hash) {
                    buf
                } else {
                    let mut buf: Vec<u8> = Vec::new();
                    buf.resize(source.size, 0);
                    target.read_chunk(hash, source.offset, &mut buf[..]).await?;
                    buf
                };
                target.write_chunk(hash, &dest[..], &buf[..]).await?;
                total_moved += source.size as u64;
                chunks.remove(hash);
            }
            ReorderOp::StoreInMem { hash, source } => {
                if !temp_store.contains_key(hash) {
                    let mut buf: Vec<u8> = Vec::new();
                    buf.resize(source.size, 0);
                    target.read_chunk(hash, source.offset, &mut buf[..]).await?;
                    temp_store.insert(hash, buf);
                }
            }
        }
    }
    Ok(total_moved + in_place_total_size)
}

/// Clone chunks from a readable source
pub async fn clone_from_readable<I>(
    opts: &CloneOptions,
    input: &mut I,
    archive: &Archive,
    chunks: &mut ChunkIndex,
    output: &mut dyn CloneOutput,
) -> Result<u64, Error>
where
    I: AsyncRead + Unpin,
{
    let mut total_read = 0;
    let hash_length = archive.chunk_hash_length();
    let seed_chunker = Chunker::new(archive.chunker_config(), input);
    let mut found_chunks = seed_chunker
        .map(|result| {
            tokio::task::spawn(async move {
                result.map(|(_offset, chunk)| {
                    (HashSum::b2_digest(&chunk, hash_length as usize), chunk)
                })
            })
        })
        .buffered(opts.get_max_buffers())
        .filter_map(|result| {
            // Filter unique chunks to be compressed
            future::ready(match result {
                Ok(Ok((hash, chunk))) => {
                    if chunks.remove(&hash) {
                        Some(Ok((hash, chunk)))
                    } else {
                        None
                    }
                }
                Ok(Err(err)) => Some(Err(err)),
                Err(err) => Some(Err(err.into())),
            })
        });

    while let Some(result) = found_chunks.next().await {
        let (hash, chunk) = result?;
        debug!("Chunk '{}', size {} used", hash, chunk.len());
        let offsets: Vec<u64> = archive
            .source_index()
            .offsets(&hash)
            .unwrap_or_else(|| panic!("missing chunk ({}) in source!?", hash))
            .collect();
        output.write_chunk(&hash, &offsets[..], &chunk).await?;
        total_read += chunk.len() as u64;
    }
    Ok(total_read)
}

/// Clone chunks from archive
pub async fn clone_from_archive(
    opts: &CloneOptions,
    reader: &mut dyn Reader,
    archive: &Archive,
    chunks: &mut ChunkIndex,
    output: &mut dyn CloneOutput,
) -> Result<u64, Error> {
    let mut total_fetched = 0u64;
    let grouped_chunks = archive.grouped_chunks(&chunks);
    for group in grouped_chunks {
        // For each group of chunks
        let start_offset = archive.chunk_data_offset() + group[0].archive_offset;
        let compression = archive.chunk_compression();
        let archive_chunk_stream = reader
            .read_chunks(
                start_offset,
                group.iter().map(|c| c.archive_size as usize).collect(),
            )
            .enumerate()
            .map(|(index, read_result)| {
                let checksum = group[index].checksum.clone();
                let source_size = group[index].source_size as usize;
                if let Ok(chunk) = &read_result {
                    total_fetched += chunk.len() as u64;
                }
                tokio::task::spawn(async move {
                    let chunk = read_result?;
                    Ok::<_, Error>((
                        checksum.clone(),
                        Archive::decompress_and_verify(compression, &checksum, source_size, chunk)?,
                    ))
                })
            })
            .buffered(opts.get_max_buffers());

        pin_mut!(archive_chunk_stream);
        while let Some(result) = archive_chunk_stream.next().await {
            // For each chunk read from archive
            let result = result?;
            let (hash, chunk) = result?;
            let offsets: Vec<u64> = archive
                .source_index()
                .offsets(&hash)
                .unwrap_or_else(|| panic!("missing chunk ({}) in source", hash))
                .collect();
            debug!("Chunk '{}', size {} used from archive", hash, chunk.len());
            output.write_chunk(&hash, &offsets[..], &chunk).await?;
        }
    }
    Ok(total_fetched)
}
