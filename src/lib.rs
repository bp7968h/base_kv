use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use serde_derive::{Deserialize, Serialize};
use std::path::Path;
use std::io::{self, prelude::*, BufReader, ErrorKind, SeekFrom};

type  ByteString = Vec<u8>;
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

    pub fn load(&mut self) -> io::Result<()>{
        let mut f = BufReader::new(&mut self.f);

        loop {
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

    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
        todo!();
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