use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use serde_derive::{Deserialize, Serialize};
use std::path::Path;
use std::io::{self, prelude::*, BufReader, ErrorKind, SeekFrom};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;

type  ByteString = Vec<u8>;
// not guaranteed to contain valid UTF-8 text.
type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair{
    pub key: ByteString,
    pub value: ByteString,
}


//Maintains a mapping between keys and file locations
#[derive(Debug)]
pub struct BaseKV{
    f: File,
    pub index: HashMap<ByteString, u64>
}

impl BaseKV {
    pub fn open(path: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(path)?;
        
        let index = HashMap::new();

        Ok(BaseKV{f, index})
    }

    //populates the index of the ActionKV struct, mapping keys to file positions
    pub fn load(&mut self) -> io::Result<()>{
        let mut f = BufReader::new(&mut self.f);

        loop {
            //returns the current position of the cursor within the underlying stream.
            //When offset is 0, the cursor remains at its current position.
            let position = f.seek(SeekFrom::Current(0))?;
            
            let maybe_kv = BaseKV::process_record(&mut f);

            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => {
                    match err.kind() {
                        ErrorKind::UnexpectedEof => break,
                        _ => return Err(err)
                    }
                }
            };

            self.index.insert(kv.key, position);
        }

        Ok(())
    }

    //f may be any type that implements Read, such as a type that reads files, but can also be &[u8].
    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let value_len = f.read_u32::<LittleEndian>()?;
        let data_len = key_len + value_len;

        let mut data = ByteString::with_capacity(data_len as usize);

        {
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }

        debug_assert_eq!(data.len(), data_len as usize);

        let checksum = crc32::checksum_ieee(&data);
        //A checksum (a number) verifies that the bytes read from disk are the same as what was intended
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered ({:08x} != {:08x})",
                checksum, saved_checksum
            );
        }

        let value = data.split_off(key_len as usize);
        let key = data;

        Ok(KeyValuePair{key, value})
    }

    pub fn get(&self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        todo!();
    }

    pub fn delete(&self, key: &ByteStr) -> io::Result<()> {
        todo!();
    }

    pub fn insert(&self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        todo!();
    }

    pub fn update(&self, key: &ByteStr, value: &ByteStr) -> io::Result<()>  {
        todo!();
    }
}