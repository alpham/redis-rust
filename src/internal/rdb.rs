use tokio::sync::MutexGuard;

use crate::internal::storage::{DBEntry, DBEntryValueType, STORAGE};
use std::{
    collections::HashMap,
    path::Path,
    time::{Duration, UNIX_EPOCH},
};

// Create RDB reader
struct RdbReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> RdbReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u8(&mut self) -> u8 {
        let rtn = self.data[self.pos];
        self.pos += 1;
        rtn
    }

    fn read_size(&mut self) -> usize {
        let first = self.read_u8();
        match first >> 6 {
            0b00 => (first & 0x3F) as usize,
            0b01 => {
                let second = self.read_u8();
                (((first & 0x3F) as usize) << 8) | second as usize
            }
            0b10 => {
                let bytes = [
                    self.read_u8(),
                    self.read_u8(),
                    self.read_u8(),
                    self.read_u8(),
                ];
                u32::from_be_bytes(bytes) as usize
            }
            _ => unreachable!("0b11 prefix is integer-string encoding, not a size"),
        }
    }

    fn read_string(&mut self) -> String {
        let marker = self.data[self.pos];
        if marker >> 6 == 0b11 {
            self.read_u8();
            match marker & 0x3f {
                0 => {
                    let value = self.read_u8();
                    value.to_string()
                }
                1 => {
                    let bytes = [self.read_u8(), self.read_u8()];
                    u16::from_le_bytes(bytes).to_string()
                }
                2 => {
                    let bytes = [
                        self.read_u8(),
                        self.read_u8(),
                        self.read_u8(),
                        self.read_u8(),
                    ];
                    u32::from_le_bytes(bytes).to_string()
                }
                _ => unreachable!("C3 is LZF compression, not used in this challenge"),
            }
        } else {
            let len = self.read_size();
            let bytes = &self.data[self.pos..self.pos + len];
            let string = String::from_utf8_lossy(bytes).into_owned();
            self.pos += len;
            string
        }
    }
}

pub async fn load_rdb(dir: &Path, dbfilename: &str) {
    let path = dir.join(dbfilename);
    let data = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(_) => return,
    };

    let mut reader = RdbReader::new(&data);
    reader.pos = 9;
    let mut storage = STORAGE.lock().await;
    let mut expiration: Option<u64> = None;

    loop {
        let opcode = reader.read_u8();
        match opcode {
            0xFA => {
                let _ = reader.read_string();
                let _ = reader.read_string();
            }
            0xFE => {
                let _ = reader.read_size();
            }
            0xFB => {
                let _ = reader.read_size();
                let _ = reader.read_size();
            }
            0xFC => {
                let bytes = [
                    reader.read_u8(),
                    reader.read_u8(),
                    reader.read_u8(),
                    reader.read_u8(),
                    reader.read_u8(),
                    reader.read_u8(),
                    reader.read_u8(),
                    reader.read_u8(),
                ];
                expiration = Some(u64::from_le_bytes(bytes));
            }
            0xFD => {
                let bytes = [
                    reader.read_u8(),
                    reader.read_u8(),
                    reader.read_u8(),
                    reader.read_u8(),
                ];
                let secs = u32::from_le_bytes(bytes);
                expiration = Some(secs as u64 * 1000);
            }
            0x00 => {
                create_value(&mut storage, &mut reader, expiration);
                expiration = None;
            }
            0xFF => break,
            _ => break,
        }
    }
}

fn create_value(
    storage: &mut MutexGuard<'_, HashMap<String, DBEntry>>,
    reader: &mut RdbReader,
    expiration_time: Option<u64>,
) {
    let key = reader.read_string();
    let value = reader.read_string();
    let mut db_entry = DBEntry::from_string(&value, DBEntryValueType::StringType);
    if let Some(ex_time) = expiration_time {
        db_entry.set_expiry_at(UNIX_EPOCH + Duration::from_millis(ex_time));
    }
    storage.insert(key, db_entry);
}
