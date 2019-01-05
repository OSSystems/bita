use curl::easy::Easy;
use std::io;

use archive_reader::ArchiveBackend;
use errors::*;

pub struct RemoteReader {
    base_url: String,
    dictionary_file_name: String,
    handle: curl::easy::Easy,
    read_offset: u64,
}

impl RemoteReader {
    pub fn new(url: &str) -> Self {
        let handle = Easy::new();
        let url_parts: Vec<&str> = url.split('/').collect();
        let url_parts_len = url_parts.len();
        let dictionary_file_name = url_parts[url_parts_len - 1].to_string();
        let base_url: Vec<&str> = url_parts.into_iter().take(url_parts_len - 1).collect();
        let base_url = base_url.join("/");

        println!(
            "base_url='{}', dictionary_file_name='{}'",
            base_url, dictionary_file_name
        );

        RemoteReader {
            dictionary_file_name: dictionary_file_name.to_string(),
            base_url,
            handle,
            read_offset: 0,
        }
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        io::Error::new(io::ErrorKind::Other, format!("{}", error))
    }
}

impl io::Read for RemoteReader {
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, io::Error> {
        // Regular read operation will read from the dictionary file
        let read_offset = self.read_offset;
        let dictionary_file_name = self.dictionary_file_name.clone();
        self.read_at(&dictionary_file_name, read_offset, buf)?;
        self.read_offset += buf.len() as u64;
        Ok(buf.len())
    }
}

impl ArchiveBackend for RemoteReader {
    fn read_at(&mut self, store_path: &str, offset: u64, buf: &mut [u8]) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let url = self.base_url.clone() + "/" + store_path;
        println!("Fetch from '{}'...", url);
        let end_offset = offset + (buf.len() - 1) as u64;
        let mut data = Vec::new();

        self.handle.url(&url).chain_err(|| "unable to set url")?;
        self.handle
            .fail_on_error(true)
            .chain_err(|| "unable to set fail on error option")?;
        self.handle
            .range(&format!("{}-{}", offset, end_offset))
            .chain_err(|| "unable to set range")?;

        {
            let mut transfer = self.handle.transfer();
            transfer
                .write_function(|new_data| {
                    data.extend_from_slice(new_data);
                    Ok(new_data.len())
                })
                .chain_err(|| "transfer write failed")?;

            transfer
                .perform()
                .chain_err(|| "failed to execute transfer")?;
        }

        buf[..data.len()].clone_from_slice(&data[..]);
        Ok(())
    }

    fn read_in_chunks<F: FnMut(Vec<u8>) -> Result<()>>(
        &mut self,
        store_path: &str,
        start_offset: u64,
        chunk_sizes: &[u64],
        mut chunk_callback: F,
    ) -> Result<()> {
        let tot_size: u64 = chunk_sizes.iter().sum();

        // Create get request
        let url = self.base_url.clone() + "/" + store_path;
        println!("Fetch from '{}'...", url);
        let mut chunk_buf: Vec<u8> = vec![];
        let mut chunk_index = 0;
        let mut total_read = 0;
        let end_offset = start_offset + tot_size - 1;

        self.handle.url(&url).chain_err(|| "unable to set url")?;
        self.handle
            .fail_on_error(true)
            .chain_err(|| "unable to set fail on error option")?;
        self.handle
            .range(&format!("{}-{}", start_offset, end_offset))
            .chain_err(|| "unable to set range")?;

        let mut transfer_result = Ok(());
        {
            let mut transfer = self.handle.transfer();
            transfer
                .write_function(|new_data| {
                    // Got data back from server
                    total_read += new_data.len();
                    chunk_buf.extend_from_slice(new_data);

                    while chunk_index < chunk_sizes.len()
                        && chunk_buf.len() >= chunk_sizes[chunk_index] as usize
                    {
                        // Got a full chunk
                        let chunk_size = chunk_sizes[chunk_index] as usize;
                        transfer_result = chunk_callback(chunk_buf.drain(..chunk_size).collect());
                        if transfer_result.is_err() {
                            // TODO: Strange error to return here but the only one available?
                            return Err(curl::easy::WriteError::Pause);
                        }
                        chunk_index += 1;
                    }
                    Ok(new_data.len())
                })
                .chain_err(|| "transfer write failed")?;
            transfer
                .perform()
                .chain_err(|| "failed to execute transfer")?;
        }

        if chunk_index < chunk_sizes.len() {
            bail!(
                "fetched {}/{} chunks ({}/{} bytes)",
                chunk_index,
                chunk_sizes.len(),
                total_read,
                chunk_sizes.iter().sum::<u64>()
            )
        }

        transfer_result
    }
}
