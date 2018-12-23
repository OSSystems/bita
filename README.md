## What

bita is a tool aiming for fast file synchronization over http.

The application works by searching for similar chunks of data in the remote archive and local seed files, and only download the chunks not present locally.

On compression the source is scanned for chunks using a rolling hash for deciding where chunks starts and ends.
Chunks are compressed and duplicated chunks are removed, then stored in either a chunk directory or a chunk archive.
A dictionary which describes how to rebuild the source file is also created along the chunk store.

On fetch the chunk dictionary is first downloaded, then bita scans the given seed files for chunks which are in the dictionary.
Any matching chunk found in a seed will be inserted into the output file.
When all the given seeds has been consumed the chunks still missing is downloaded from the remote archive.

---

The file to update could be any file where data is expected to only change partially between updates.
Any local file that might contain data of the source file may be used as seed while unpacking.
bita can also use its own archives as seed input.

In an update system with a A/B partition setup one could use bita to update the B partition while using the A partition as seed. This should result in a download only of the chunks that differ between partition A and the source file.

As default bita compress chunk data using lzma at level 6. No compression and zstd compression is also supported.

### Similar Tools
* [casync](https://github.com/systemd/casync)
* [zchunk](https://github.com/zchunk/zchunk)
* [zsync](http://zsync.moria.org.uk)
* [rsync](https://rsync.samba.org/)


### Meaning
* Source file - Any file we wish to synchronize. Typically not a compressed file.
* Dictionary - A description of how to rebuild a source file using a chunk store.
* Chunk store - A collection of chunks, either as a chunk directory or a chunk archive.
* Chunk directory - Chunks stored as a separate file per chunk.
* Chunk archive - Chunks stored in a single file.

### Operation
Bita allows for three main operations:
* compress - Create a dictionary+archive/chunk store from a source file.
* fetch - Donwload an archive. Can use local seeds. Can also unpack the archive to a (seekable) file on the fly.
* unpack - Unpack a local archive. Can not use seeds. Can write to stdout.


## Building
`$ cargo build` or `$ cargo build --release`

## Example usage

##### Compress stream from stdin
`olle@host:~$ gunzip -c file.gz | bita compress --compression ZSTD --compression-level 9 file.cba`

##### Compress a file
`olle@host:~$ bita compress file.ext4 file.ext4.cba`

##### Fetch archive using multiple seeds
`olle@device:~$ gunzip -c old.tar.gz | bita fetch --seed an_old.cba --seed another_old.tar http://host/file.cba file.cba`

##### Fetch and decompress using block device as seed and target
`olle@device:~$ bita fetch --seed /dev/disk/by-partlabel/rootfs-A --unpack http://host/file.ext4.cba /dev/disk/by-partlabel/rootfs-B`


##### Unpack a local archive
`olle@device:~$ bita unpack file.ext4.cba file.ext4`
