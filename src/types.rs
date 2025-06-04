use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use strum::VariantNames;
use strum_macros::VariantNames;

#[derive(Debug)]
pub struct ParseStorageMethodError(String);

impl Display for ParseStorageMethodError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseStorageMethodError {}

impl FromStr for StorageMethod {
    type Err = ParseStorageMethodError;

    /// Get chain from string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Put" => Ok(Self::Put),
            "ChildPut" => Ok(Self::ChildPut),
            "ChildKill" => Ok(Self::ChildKill),
            "ClearPrefix" => Ok(Self::ClearPrefix),
            "ChildClearPrefix" => Ok(Self::ChildClearPrefix),
            "Append" => Ok(Self::Append),
            "Genesis" => Ok(Self::Genesis),
            _ => Err(ParseStorageMethodError(format!(
                "Unknown storage method: {s}"
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, VariantNames)]
pub enum StorageMethod {
    Put,
    ChildPut,
    ChildKill,
    ClearPrefix,
    ChildClearPrefix,
    Append,
    Genesis,
}

impl Display for StorageMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Put => "Put",
            Self::ChildPut => "ChildPut",
            Self::ChildKill => "ChildKill",
            Self::ClearPrefix => "ClearPrefix",
            Self::ChildClearPrefix => "ChildClearPrefix",
            Self::Append => "Append",
            Self::Genesis => "Genesis",
        };
        write!(f, "{str}")
    }
}

impl StorageMethod {
    pub fn names() -> Vec<String> {
        StorageMethod::VARIANTS
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct BlockTrace {
    pub index: u32,
    pub key: String,
    pub value: String,
    pub ext_id: String,
    pub method: StorageMethod,
    pub parent_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BlockTraces {
    pub block_hash: String,
    pub block_parent_hash: String,
    pub block_number: u64,
    pub runtime_version: u32,
    pub is_finalized: bool,
    pub traces: Vec<BlockTrace>,
}
