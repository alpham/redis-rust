use core::str;
use std::{
    collections::HashMap,
    fmt::Display,
    sync::Mutex,
    time::{Duration, SystemTime},
};

use crate::internal::commands::CommandError::StorageError;

use super::commands::CommandError;

lazy_static! {
    pub static ref STORAGE: Mutex<HashMap<String, DBEntry>> = Mutex::new(HashMap::new());
}

pub struct DBEntry {
    item: DBEntryValue,
    metadata: DBEntryMetadata,
}

impl DBEntry {
    pub fn from_string(value: &str, value_type: DBEntryValueType) -> Self {
        // TODO: add check for `px` parameter
        DBEntry {
            item: DBEntryValue {
                value: value.to_string(),
                type_: value_type,
            },
            metadata: DBEntryMetadata {
                created_at: SystemTime::now(),
                ttl: None,
            },
        }
    }

    pub fn to_string(&self) -> Result<String, CommandError> {
        if self.still_valid() {
            return Ok(self.item.value.clone());
        }

        Err(StorageError("Value has expired".to_string()))
    }

    pub fn set_ttl(&mut self, duration_str: Option<&String>) -> Result<(), CommandError> {
        if let Some(duration_str) = duration_str {
            if let Ok(millis) = duration_str.parse::<u64>() {
                let duration = Duration::from_millis(millis);
                self.metadata.ttl = Some(duration);
                Ok(())
            } else {
                Err(CommandError::InvalidArgument(format!(
                    "Invalid duration provided '{}'",
                    duration_str
                )))
            }
        } else {
            Err(CommandError::InvalidArgument(
                "Duration was not provided".to_string(),
            ))
        }
    }

    fn still_valid(&self) -> bool {
        if let Some(ttl) = self.metadata.ttl {
            println!("found ttl --> {}", ttl.as_millis());
            let expiry_time = self.metadata.created_at + ttl;
            if SystemTime::now() > expiry_time {
                return false;
            }
        }
        true
    }
}

impl Display for DBEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}{}", self.item.type_, self.item.value)
    }
}

pub struct DBEntryValue {
    value: String,
    type_: DBEntryValueType,
}

#[derive(Debug)]
pub enum DBEntryValueType {
    StringType,
    _ListType,
}

#[derive(Debug, Clone, Copy)]
pub struct DBEntryMetadata {
    ttl: Option<Duration>,
    created_at: SystemTime,
}
