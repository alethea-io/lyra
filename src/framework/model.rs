use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;

use pallas::ledger::traverse::{Era, MultiEraOutput, MultiEraTx, OutputRef};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::crosscut::policies::{AppliesPolicy, RuntimePolicy};

use super::errors::Error;

#[derive(Default, Debug, Clone)]
pub struct BlockContext {
    utxos: HashMap<String, (Era, Vec<u8>)>,
}
impl BlockContext {
    pub fn import_ref_output(&mut self, key: &OutputRef, era: Era, cbor: Vec<u8>) {
        self.utxos.insert(key.to_string(), (era, cbor));
    }

    pub fn find_utxo(&self, key: &OutputRef) -> Result<MultiEraOutput, Error> {
        let (era, cbor) = self
            .utxos
            .get(&key.to_string())
            .ok_or_else(|| Error::missing_utxo(key))?;

        MultiEraOutput::decode(*era, cbor).map_err(Error::cbor)
    }

    pub fn get_all_keys(&self) -> Vec<String> {
        self.utxos.keys().map(|x| x.clone()).collect()
    }

    pub fn find_consumed_txos(
        &self,
        tx: &MultiEraTx,
        policy: &RuntimePolicy,
    ) -> Result<Vec<(OutputRef, MultiEraOutput)>, Error> {
        let items = tx
            .consumes()
            .iter()
            .map(|i| i.output_ref())
            .map(|r| self.find_utxo(&r).map(|u| (r, u)))
            .map(|r| r.apply_policy(policy))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(items)
    }
}

pub type Set = String;
pub type Member = String;
pub type Key = String;
pub type Delta = i64;
pub type Timestamp = u64;

#[derive(Clone, Debug, Deserialize)]
pub enum Value {
    String(String),
    BigInt(i128),
    Cbor(Vec<u8>),
    Json(serde_json::Value),
}

impl From<String> for Value {
    fn from(x: String) -> Self {
        Value::String(x)
    }
}

impl From<Vec<u8>> for Value {
    fn from(x: Vec<u8>) -> Self {
        Value::Cbor(x)
    }
}

impl From<serde_json::Value> for Value {
    fn from(x: serde_json::Value) -> Self {
        Value::Json(x)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[non_exhaustive]
pub enum CRDTCommand {
    SetAdd(Set, Member),
    SetRemove(Set, Member),
    SortedSetAdd(Set, Member, Delta),
    SortedSetRemove(Set, Member, Delta),
    TwoPhaseSetAdd(Set, Member),
    TwoPhaseSetRemove(Set, Member),
    GrowOnlySetAdd(Set, Member),
    LastWriteWins(Key, Value, Timestamp),
    AnyWriteWins(Key, Value),
    // TODO make sure Value is a generic not stringly typed
    PNCounter(Key, Delta),
    HashCounter(Key, Member, Delta),
    HashSetValue(Key, Member, Value),
    HashUnsetKey(Key, Member),
}

impl CRDTCommand {
    pub fn set_add(prefix: Option<&str>, key: &str, member: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SetAdd(key, member)
    }

    pub fn set_remove(prefix: Option<&str>, key: &str, member: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SetRemove(key, member)
    }

    pub fn sorted_set_add(
        prefix: Option<&str>,
        key: &str,
        member: String,
        delta: i64,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SortedSetAdd(key, member, delta)
    }

    pub fn sorted_set_remove(
        prefix: Option<&str>,
        key: &str,
        member: String,
        delta: i64,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SortedSetRemove(key, member, delta)
    }

    pub fn any_write_wins<K, V>(prefix: Option<&str>, key: K, value: V) -> CRDTCommand
    where
        K: ToString,
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::AnyWriteWins(key, value.into())
    }

    pub fn last_write_wins<V>(
        prefix: Option<&str>,
        key: &str,
        value: V,
        ts: Timestamp,
    ) -> CRDTCommand
    where
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::LastWriteWins(key, value.into(), ts)
    }

    pub fn hash_set_value<V>(
        prefix: Option<&str>,
        key: &str,
        member: String,
        value: V,
    ) -> CRDTCommand
    where
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::HashSetValue(key, member, value.into())
    }

    pub fn hash_del_key(prefix: Option<&str>, key: &str, member: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::HashUnsetKey(key, member)
    }

    pub fn hash_counter(
        prefix: Option<&str>,
        key: &str,
        member: String,
        delta: i64,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::HashCounter(key, member, delta)
    }

    pub fn from_json(value: &JsonValue) -> Result<CRDTCommand, String> {
        let obj = value.as_object().ok_or("Expected a JSON object")?;
    
        match obj.get("command").and_then(JsonValue::as_str) {
            Some("SetAdd") => {
                let set = extract_string(obj, "set")?;
                let member = extract_string(obj, "member")?;
                Ok(CRDTCommand::SetAdd(set, member))
            }
            Some("SetRemove") => {
                let set = extract_string(obj, "set")?;
                let member = extract_string(obj, "member")?;
                Ok(CRDTCommand::SetRemove(set, member))
            }
            Some("SortedSetAdd") => {
                let set = extract_string(obj, "set")?;
                let member = extract_string(obj, "member")?;
                let delta = extract_delta(obj, "delta")?;
                Ok(CRDTCommand::SortedSetAdd(set, member, delta))
            }
            Some("SortedSetRemove") => {
                let set = extract_string(obj, "set")?;
                let member = extract_string(obj, "member")?;
                let delta = extract_delta(obj, "delta")?;
                Ok(CRDTCommand::SortedSetRemove(set, member, delta))
            }
            Some("AnyWriteWins") => {
                let key = extract_string(obj, "key")?;
                let value = extract_value(obj, "value")?;
                Ok(CRDTCommand::AnyWriteWins(key, value))
            }
            Some("LastWriteWins") => {
                let key = extract_string(obj, "key")?;
                let value = extract_value(obj, "value")?;
                let ts = extract_timestamp(obj, "timestamp")?;
                Ok(CRDTCommand::LastWriteWins(key, value, ts))
            }
            Some("PNCounter") => {
                let key = extract_string(obj, "key")?;
                let delta = extract_delta(obj, "value")?;
                Ok(CRDTCommand::PNCounter(key, delta))
            }
            Some("HashCounter") => {
                let key = extract_string(obj, "key")?;
                let member = extract_string(obj, "member")?;
                let delta = extract_delta(obj, "delta")?;
                Ok(CRDTCommand::HashCounter(key, member, delta))
            }
            Some("HashSetValue") => {
                let key = extract_string(obj, "key")?;
                let member = extract_string(obj, "member")?;
                let value = extract_value(obj, "value")?;
                Ok(CRDTCommand::HashSetValue(key, member, value))
            }
            Some("HashUnsetKey") => {
                let key = extract_string(obj, "key")?;
                let member = extract_string(obj, "member")?;
                Ok(CRDTCommand::HashUnsetKey(key, member))
            }
            _ => Err("Unknown CRDTCommand".into()),
        }
    }

    pub fn from_json_array(value: &JsonValue) -> Result<Vec<CRDTCommand>, String> {
        let commands = value
            .as_array()
            .ok_or("Expected a JSON array")?
            .iter()
            .map(CRDTCommand::from_json)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(commands)
    }
}

fn extract_string(obj: &serde_json::Map<String, JsonValue>, key: &str) -> Result<String, String> {
    obj.get(key)
        .and_then(JsonValue::as_str)
        .map(String::from)
        .ok_or_else(|| format!("Expected a string for key {}", key))
}

fn extract_delta(obj: &serde_json::Map<String, JsonValue>, key: &str) -> Result<i64, String> {
    match obj.get(key) {
        Some(JsonValue::Number(num)) if num.is_i64() => num
            .as_i64()
            .ok_or_else(|| format!("Expected an integer delta for key {}", key)),
        Some(JsonValue::String(s)) => i64::from_str(s)
            .map_err(|_| format!("Failed to parse stringified integer for key {}", key)),
        _ => Err(format!(
            "Expected an integer or stringified integer delta for key {}",
            key
        )),
    }
}

fn extract_timestamp(obj: &serde_json::Map<String, JsonValue>, key: &str) -> Result<u64, String> {
    obj.get(key)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| format!("Expected a timestamp for key {}", key))
}

fn extract_value(obj: &serde_json::Map<String, JsonValue>, key: &str) -> Result<Value, String> {
    obj.get(key)
        .cloned()
        .map(Value::Json)
        .ok_or_else(|| format!("Expected a value for key {}", key))
}