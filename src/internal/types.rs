use std::{
    any::Any,
    collections::BTreeMap,
    fmt::{Display, Formatter, Result as FmtResult},
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
    millis: u64,
    seq: u64,
}

fn invalid_id() -> CommandError {
    CommandError::InvalidArgument(
        "Invalid stream ID specified as stream command argument".to_string(),
    )
}

impl StreamId {
    pub fn parse(s: &str) -> Result<Self, CommandError> {
        let (ms, seq) = s.split_once("-").ok_or_else(invalid_id)?;

        let id = StreamId {
            millis: ms.parse().map_err(|_| invalid_id())?,
            seq: seq.parse().map_err(|_| invalid_id())?,
        };

        if id.millis == 0 && id.seq == 0 {
            return Err(CommandError::InvalidArgument(
                "The ID specified in XADD must be greater than 0-0".to_string(),
            ));
        }

        Ok(id)
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
