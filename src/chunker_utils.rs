use ordered_mpsc::OrderedMPSC;
use std::collections::{hash_map::Entry, HashMap};
use std::io;
use std::io::prelude::*;
use threadpool::ThreadPool;

use chunker::*;

#[derive(Debug, Clone)]
pub struct HashedChunk {
    pub hash: Vec<u8>,
    pub chunk: Chunk,
}

#[derive(Debug, Clone)]
pub struct CompressedChunk {
    pub hash: Vec<u8>,
    pub chunk: Chunk,
    pub cdata: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ChunkDesc {
    pub offset: usize,
    pub size: usize,
    pub hash: Vec<u8>,
}

// Calculate a strong hash on every chunk and forward each chunk
// Returns an array of chunk index.
pub fn chunk_and_hash<T, F, H>(
    src: &mut T,
    mut chunker: Chunker,
    hash_chunk: H,
    pool: &ThreadPool,
    mut result: F,
) -> io::Result<(Vec<ChunkDesc>)>
where
    T: Read,
    F: FnMut(HashedChunk),
    H: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
{
    let mut chunks: Vec<ChunkDesc> = Vec::new();
    let mut chunk_channel = OrderedMPSC::new();
    chunker
        .scan(src, |chunk| {
            // For each chunk in file
            let chunk = chunk.clone();
            let chunk_tx = chunk_channel.new_tx();
            pool.execute(move || {
                let hash = hash_chunk(&chunk.data);
                chunk_tx
                    .send((
                        ChunkDesc {
                            hash: hash.clone(),
                            offset: chunk.offset,
                            size: chunk.data.len(),
                        },
                        HashedChunk {
                            hash: hash,
                            chunk: chunk,
                        },
                    )).expect("chunk_tx");
            });

            chunk_channel
                .rx()
                .try_iter()
                .for_each(|(chunk_desc, hashed_chunk)| {
                    result(hashed_chunk);
                    chunks.push(chunk_desc);
                });
        }).expect("chunker");

    // Wait for threads to be done
    pool.join();

    // Forward the last hashed chunks
    chunk_channel
        .rx()
        .try_iter()
        .for_each(|(chunk_desc, hashed_chunk)| {
            result(hashed_chunk);
            chunks.push(chunk_desc);
        });
    Ok(chunks)
}

// Iterate only unique chunks of a source
pub fn unique_chunks<T, F, H>(
    src: &mut T,
    chunker: Chunker,
    hash_chunk: H,
    pool: &ThreadPool,
    mut result: F,
) -> io::Result<Vec<ChunkDesc>>
where
    T: Read,
    F: FnMut(HashedChunk),
    H: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
{
    let mut chunk_map: HashMap<Vec<u8>, usize> = HashMap::new();
    return chunk_and_hash(
        src,
        chunker,
        hash_chunk,
        pool,
        |hashed_chunk| match chunk_map.entry(hashed_chunk.hash.clone()) {
            Entry::Occupied(o) => {
                (*o.into_mut()) += 1;
            }
            Entry::Vacant(v) => {
                v.insert(1);
                result(hashed_chunk);
            }
        },
    );
}

// Iterate unique and compressed chunks
pub fn unique_compressed_chunks<T, F, C, H>(
    src: &mut T,
    chunker: Chunker,
    hash_chunk: H,
    compress_chunk: C,
    pool: &ThreadPool,
    mut result: F,
) -> io::Result<Vec<ChunkDesc>>
where
    T: Read,
    F: FnMut(CompressedChunk),
    C: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
    H: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
{
    let mut chunk_channel = OrderedMPSC::new();
    let chunks = unique_chunks(src, chunker, hash_chunk, &pool, |hashed_chunk| {
        // For each unique chunk
        let chunk_tx = chunk_channel.new_tx();
        pool.execute(move || {
            // Compress the chunk (in thread context)
            let cdata = compress_chunk(&hashed_chunk.chunk.data);
            chunk_tx
                .send(CompressedChunk {
                    hash: hashed_chunk.hash,
                    chunk: hashed_chunk.chunk,
                    cdata: cdata,
                }).expect("chunk_tx");
        });

        chunk_channel.rx().try_iter().for_each(|compressed_chunk| {
            result(compressed_chunk);
        });
    })?;

    // Wait for threads to be done
    pool.join();

    // Forward the compressed chunks
    chunk_channel.rx().try_iter().for_each(|compressed_chunk| {
        result(compressed_chunk);
    });
    Ok(chunks)
}
