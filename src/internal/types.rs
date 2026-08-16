use std::{
    any::Any,
    collections::BTreeMap,
    fmt::{Display, Formatter, Result as FmtResult},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::internal::commands::CommandError;

pub trait DBValue: Sync + Send + Display {
    fn type_name(&self) -> &'static str;
    fn len(&self) -> usize;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl DBValue for String {
    fn len(&self) -> usize {
        self.len()
    }

    fn type_name(&self) -> &'static str {
        "string"
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// StreamId implementation
/// StreamId is meant for parsing and retrieving a stream id.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub millis: u64,
    pub seq: u64,
}

fn invalid_id() -> CommandError {
    CommandError::InvalidArgument(
        "Invalid stream ID specified as stream command argument".to_string(),
    )
}

impl Display for StreamId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}-{}", self.millis, self.seq)
    }
}

// StreamType implementation
#[derive(Debug, Default, Clone)]
pub struct StreamType {
    entries: BTreeMap<StreamId, Vec<(String, String)>>,
}

impl DBValue for StreamType {
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn type_name(&self) -> &'static str {
        "stream"
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Display for StreamType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} entries", self.entries.len())
    }
}

impl StreamType {
    ///Sequence number to use when the caller writes `<ms>-*`.
    fn next_seq(&self, millis: u64) -> u64 {
        match self.entries.last_key_value() {
            Some((last, _)) if last.millis == millis => last.seq + 1,
            Some(_) => 0,
            None if millis == 0 => 1,
            None => 0,
        }
    }

    pub fn parse_stream_id(&self, s: &str) -> Result<StreamId, CommandError> {
        let millis;
        let seq;
        if s == "*" {
            millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            seq = self.next_seq(millis);
        } else {
            let (ms_str, seq_str) = s.split_once('-').ok_or_else(invalid_id)?;
            millis = ms_str.parse().map_err(|_| invalid_id())?;
            seq = match seq_str {
                "*" => self.next_seq(millis),
                other => other.parse().map_err(|_| invalid_id())?,
            };
            if millis == 0 && seq == 0 {
                return Err(CommandError::InvalidArgument(
                    "The ID specified in XADD must be greater than 0-0".to_string(),
                ));
            }
        }

        Ok(StreamId { millis, seq })
    }

    pub fn add(
        &mut self,
        id: StreamId,
        fields: Vec<(String, String)>,
    ) -> Result<StreamId, CommandError> {
        if let Some((last, _)) = self.entries.last_key_value() {
            if id <= *last {
                return Err(CommandError::InvalidArgument(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                ));
            }
        }
        self.entries.insert(id, fields);
        Ok(id)
    }
}
