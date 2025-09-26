use walrus::wal::{Walrus, WalIndex};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write, Read};

fn cleanup_wal() {
    let _ = fs::remove_dir_all("wal_files");
}

fn first_data_file() -> String {
    let mut files: Vec<_> = fs::read_dir("./wal_files").unwrap().flatten().collect();
    files.sort_by_key(|e| e.file_name());
    let p = files
        .into_iter()
        .find(|e| !e.file_name().to_string_lossy().ends_with("_index.db"))
        .unwrap()
        .path();
    p.to_string_lossy().to_string()
}

#[test]
fn walindex_persists() {
    fs::create_dir_all("wal_files").unwrap();
    let name = format!("unit_idx_{}", {
        use std::time::SystemTime;
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis()
    });
    let mut idx = WalIndex::new(&name);
    idx.set("k".to_string(), 7, 99);
    drop(idx);
    let idx2 = WalIndex::new(&name);
    let bp = idx2.get("k").unwrap();
    assert_eq!(bp.cur_block_idx, 7);
    assert_eq!(bp.cur_block_offset, 99);
    cleanup_wal();
}

// #[test]
// fn basic_roundtrip_single_topic() {
//     cleanup_wal();
//     let wal = Walrus::new();
//     wal.append_for_topic("t", b"x").unwrap();
//     wal.append_for_topic("t", b"y").unwrap();
//     assert_eq!(wal.read_next("t").unwrap().data, b"x");
//     assert_eq!(wal.read_next("t").unwrap().data, b"y");
//     assert!(wal.read_next("t").is_none());
//     cleanup_wal();
// }

// #[test]
// fn basic_roundtrip_multi_topic() {
//     cleanup_wal();
//     let wal = Walrus::new();
//     wal.append_for_topic("a", b"1").unwrap();
//     wal.append_for_topic("b", b"2").unwrap();
//     assert_eq!(wal.read_next("a").unwrap().data, b"1");
//     assert_eq!(wal.read_next("b").unwrap().data, b"2");
//     cleanup_wal();
// }

// #[test]
// fn persists_read_offsets_across_restart() {
//     cleanup_wal();
//     let wal = Walrus::new();
//     wal.append_for_topic("t", b"a").unwrap();
//     wal.append_for_topic("t", b"b").unwrap();
//     assert_eq!(wal.read_next("t").unwrap().data, b"a");
//     // restart
//     let wal2 = Walrus::new();
//     assert_eq!(wal2.read_next("t").unwrap().data, b"b");
//     assert!(wal2.read_next("t").is_none());
//     cleanup_wal();
// }

#[test]
fn checksum_corruption_is_detected_via_public_api() {
    cleanup_wal();
    let wal = Walrus::new();
    wal.append_for_topic("t", b"abcdef").unwrap();
    // corrupt by finding the data pattern in the file and flipping a byte
    let path = first_data_file();
    let mut bytes = Vec::new();
    {
        let mut f = OpenOptions::new().read(true).open(&path).unwrap();
        f.read_to_end(&mut bytes).unwrap();
    }
    if let Some(pos) = bytes.windows(6).position(|w| w == b"abcdef") {
        // flip one byte in the middle of the payload
        let flip_pos = pos + 2;
        let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(flip_pos as u64)).unwrap();
        f.write_all(&[bytes[flip_pos] ^ 0xFF]).unwrap();
    } else {
        panic!("payload not found to corrupt");
    }
    // restart and try reading
    let wal2 = Walrus::new();
    let res = wal2.read_next("t");
    assert!(res.is_none());
    cleanup_wal();
}
