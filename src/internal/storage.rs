use tokio::sync::Notify;

use crate::internal::{
    commands::CommandError::StorageError,
    types::{DBValue, StreamType},
};
use core::str;
use std::sync::Mutex;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use super::commands::CommandError;

lazy_static! {
    pub static ref STORAGE: Mutex<HashMap<String, DBEntry>> = Mutex::new(HashMap::new());
    /// Signalled after any stream gains a new entry. Blocked XREAD clients wait on this.
    pub static ref STREAM_ADDED: Notify = Notify::new();
}

pub fn with_storage<R>(f: impl FnOnce(&mut HashMap<String, DBEntry>) -> R) -> R {
    let mut storage = STORAGE.lock().unwrap_or_else(|e| e.into_inner());
    f(&mut storage)
}

pub struct DBEntry {
    item: Box<dyn DBValue>,
    metadata: DBEntryMetadata,
}

impl DBEntry {
    pub fn from_string(value: &str) -> Self {
        // TODO: add check for `px` parameter
        DBEntry {
            item: Box::new(value.to_string()),
            metadata: DBEntryMetadata { expire_at: None },
        }
    }

    pub fn from_stream(value: StreamType) -> Self {
        DBEntry {
            item: Box::new(value),
            metadata: DBEntryMetadata { expire_at: None },
        }
    }

    pub fn value(&self) -> Result<&dyn DBValue, CommandError> {
        if self.still_valid() {
            return Ok(self.item.as_ref());
        }

        Err(StorageError("Value has expired".to_string()))
    }

    pub fn value_mut(&mut self) -> Result<&mut dyn DBValue, CommandError> {
        if self.still_valid() {
            return Ok(self.item.as_mut());
        }

        Err(StorageError("Value has expired".to_string()))
    }

    pub fn set_ttl(&mut self, duration_str: Option<&String>) -> Result<(), CommandError> {
        if let Some(duration_str) = duration_str {
            if let Ok(millis) = duration_str.parse::<u64>() {
                let duration = Duration::from_millis(millis);
                self.metadata.expire_at = Some(SystemTime::now() + duration);
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

    pub fn set_expiry_at(&mut self, at: SystemTime) {
        self.metadata.expire_at = Some(at)
    }

    fn still_valid(&self) -> bool {
        if let Some(expiry_time) = self.metadata.expire_at {
            if SystemTime::now() > expiry_time {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DBEntryMetadata {
    expire_at: Option<SystemTime>,
}
