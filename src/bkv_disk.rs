use libbase_kv::BaseKV;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use bincode;

#[cfg(target_os = "windows")]
const USAGE: &str = "
Usage:
    akv_mem.exe FILE get KEY
    akv_mem.exe FILE delete KEY
    akv_mem.exe FILE insert KEY VALUE
    akv_mem.exe FILE update KEY VALUE
";

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage:
    akv_mem FILE get KEY
    akv_mem FILE delete KEY
    akv_mem FILE insert KEY VALUE
    akv_mem FILE update KEY VALUE
";

type  ByteString = Vec<u8>;
// not guaranteed to contain valid UTF-8 text.
type ByteStr = [u8];

fn main(){
    const INDEX_KEY: &ByteStr = b"+index";

    let args: Vec<String> = env::args().collect();
    let fname = args.get(1).expect(&USAGE);
    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE).as_ref();
    let maybe_value = args.get(4);

    let path = Path::new(&fname);
    let mut a = BaseKV::open(path).expect("Unable to open file");

    a.load().expect("Unable to load data");

    match action {
        "get" => {
            let index_as_bytes = a.get(&INDEX_KEY).unwrap().unwrap();

            let index_decoded = bincode::deserialize(&index_as_bytes);
            let index: HashMap<ByteString, u64> = index_decoded.unwrap();

            match index.get(key){
                None => eprintln!("{:?} not found", key),
                Some(&i) => {
                    let kv = a.get_at(i).unwrap();
                    println!("{:?}", kv.value)
                }
            }
        },

        "delete" => a.delete(key).unwrap(),

        "insert" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            a.insert(key, value).unwrap();
            store_index_on_disk(&mut a, INDEX_KEY);
        },

        "update" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            a.update(key, value).unwrap();
            store_index_on_disk(&mut a, INDEX_KEY);
        },

        _ => eprintln!("{}", &USAGE),
    }
}

fn store_index_on_disk(a: &mut BaseKV, index_key: &ByteStr) {
    a.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&a.index).unwrap();
    a.index = std::collections::HashMap::new();
    a.insert(index_key, &index_as_bytes).unwrap();
}