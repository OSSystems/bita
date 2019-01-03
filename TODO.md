### Should

 * Come up with file type names for dictionary and archive (and maybe chunk files)!

 * compress - Should probably take output name without file ending

 * clone - should as default create a local clone of the dictionary and chunk directory/archive and optionally unpack it on the fly (current behavior).

 * unpack - Create this command which unpacks from a local dictionary.

 * Allow for fixed block size while chunking.

### Probably

 * On unpack - Add option for generating a chunk dictionary with chunk data located in the destination file.
   This to allow for lookup of old chunks without scanning on next unpack.

### Next

 * Create 'bita server' which can serve dictionaries and chunk files with less overhead than a regular http server. This as the client could do a single request of all chunks needed instead of a request for each chunk to download.
   The server could also be used directly when compressing a source file and let the client only upload the chunks not already present on the server.

