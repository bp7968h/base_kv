use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use serde_derive::{Deserialize, Serialize};
use std::path::Path;
use std::io::{self, prelude::*, BufReader, BufWriter, ErrorKind, SeekFrom};
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

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            None => return Ok(None),
            Some(position) => *position,
        };

        let kv = self.get_at(position)?;

        Ok(Some(kv.value))
    }

    pub fn get_at(&mut self, position: u64) -> io::Result<KeyValuePair> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Start(position))?;

        let kv = BaseKV::process_record(&mut f)?;

        Ok(kv)
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let position = self.insert_but_ignore_index(key, value)?;

        self.index.insert(key.to_vec(), position);

        Ok(())
    }

    fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64> {
        let mut f = BufWriter::new(&mut self.f);

        let key_len = key.len();
        let val_len = value.len();
        let mut tmp = ByteString::with_capacity(key_len + val_len);

        for byte in key{
            tmp.push(*byte);
        }

        for byte in value{
            tmp.push(*byte);
        }

        let checksum = crc32::checksum_ieee(&tmp);

        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;
        
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(val_len as u32)?;
        f.write_all(&mut tmp)?;

        Ok(current_position)
    }

    //hint to the compiler to consider inlining
    //Inlining is an optimization technique where the compiler replaces a function call with the actual code of the function.
    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()>  {
        self.insert(key, value)
    }
}