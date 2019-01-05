use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use archive_reader::*;
use errors::*;

pub struct LocalReaderBackend {
    base_path: PathBuf,
    dictionary_file_name: PathBuf,
    read_offset: u64,
}

impl LocalReaderBackend {
    pub fn new(dictionary_path: &Path) -> Self {
        LocalReaderBackend {
            dictionary_file_name: Path::new(
                dictionary_path
                    .file_name()
                    .unwrap_or_else(|| OsStr::new("")),
            )
            .to_path_buf(),
            base_path: dictionary_path
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .to_path_buf(),
            read_offset: 0,
        }
    }
}

impl io::Read for LocalReaderBackend {
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, io::Error> {
        // Regular read operation will read from the dictionary file
        let read_offset = self.read_offset;
        let dictionary_file_name = self.dictionary_file_name.clone();
        self.read_at(&dictionary_file_name, read_offset, buf)?;
        self.read_offset += buf.len() as u64;
        Ok(buf.len())
    }
}

impl ArchiveBackend for LocalReaderBackend {
    fn read_at(&mut self, store_path: &Path, offset: u64, buf: &mut [u8]) -> Result<()> {
        let path = self.base_path.join(store_path);
        let mut file =
            File::open(&path).chain_err(|| format!("unable to open {}", path.display()))?;

        file.seek(SeekFrom::Start(offset))
            .chain_err(|| "failed to seek archive file")?;
        file.read_exact(buf)
            .chain_err(|| "failed to read archive file")?;
        Ok(())
    }

    fn read_in_chunks<F: FnMut(Vec<u8>) -> Result<()>>(
        &mut self,
        store_path: &Path,
        start_offset: u64,
        chunk_sizes: &[u64],
        mut chunk_callback: F,
    ) -> Result<()> {
        let path = self.base_path.join(store_path);
        let mut file =
            File::open(&path).chain_err(|| format!("unable to open {}", path.display()))?;

        file.seek(SeekFrom::Start(start_offset))
            .chain_err(|| "failed to seek archive file")?;
        for chunk_size in chunk_sizes {
            let mut buf: Vec<u8> = vec![0; *chunk_size as usize];
            file.read_exact(&mut buf[..])
                .chain_err(|| "failed to read archive file")?;
            chunk_callback(buf)?;
        }
        Ok(())
    }
}
