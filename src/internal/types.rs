use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter, Result as FmtResult},
};

use crate::internal::commands::CommandError;

pub trait DBValue: Sync + Send + Display {
    fn type_name(&self) -> &'static str;
    fn len(&self) -> usize;
}

impl DBValue for String {
    fn len(&self) -> usize {
        self.len()
    }

    fn type_name(&self) -> &'static str {
        "string"
    }
}

// StreamId implementation
/// StreamId is meant for parsing and retrieving a stream id.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    millis: u64,
    seq: u64,
}

impl StreamId {
    pub fn parse(s: &str) -> Option<Self> {
        let (ms, seq) = s.split_once('-')?;
        Some(StreamId {
            millis: ms.parse().ok()?,
            seq: seq.parse().ok()?,
        })
    }
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
}

impl Display for StreamType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} entries", self.entries.len())
    }
}

impl StreamType {
    pub fn add(
        &mut self,
        id: StreamId,
        fields: Vec<(String, String)>,
    ) -> Result<StreamId, CommandError> {
        if let Some((last, _)) = self.entries.last_key_value() {
            if id <= *last {
                return Err(CommandError::InvalidArgument("The ID spedified in the XADD is equal or smaller than the targt stream top item".to_string()));
            }
        }
        self.entries.insert(id, fields);
        Ok(id)
    }
}
