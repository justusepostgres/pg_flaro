//! Reference key specifications and tips for the supported command catalog.
use crate::protocol::Reply;
use serde::Deserialize;
use std::{collections::HashMap, sync::OnceLock};

#[derive(Deserialize)]
struct Info {
    documentation: serde_json::Value,
    arity: i64,
    legacy: [i64; 3],
    flags: Vec<String>,
    tips: Vec<String>,
    key_specs: Vec<KeySpec>,
}
#[derive(Deserialize)]
struct KeySpec {
    notes: Option<String>,
    flags: Vec<String>,
    begin_search: HashMap<String, serde_json::Value>,
    find_keys: HashMap<String, serde_json::Value>,
}
fn catalog() -> &'static HashMap<String, Info> {
    static CATALOG: OnceLock<HashMap<String, Info>> = OnceLock::new();
    CATALOG.get_or_init(|| {
        serde_json::from_str(include_str!("command_info.json")).expect("generated command metadata")
    })
}
fn bulk(text: impl AsRef<[u8]>) -> Reply {
    Reply::Bulk(Some(text.as_ref().to_vec()))
}
fn field(name: &str, value: Reply) -> (Reply, Reply) {
    (bulk(name), value)
}
pub(super) fn tips(name: &str) -> Reply {
    Reply::Set(
        catalog()
            .get(name)
            .map(|i| i.tips.iter().map(bulk).collect())
            .unwrap_or_default(),
    )
}
pub(super) fn key_specs(name: &str) -> Reply {
    Reply::Set(
        catalog()
            .get(name)
            .map(|i| {
                i.key_specs
                    .iter()
                    .map(|s| {
                        let mut fields = Vec::new();
                        if let Some(notes) = &s.notes {
                            fields.push(field("notes", bulk(notes)));
                        }
                        let order = [
                            "RO",
                            "RW",
                            "OW",
                            "RM",
                            "ACCESS",
                            "UPDATE",
                            "INSERT",
                            "DELETE",
                            "NOT_KEY",
                            "INCOMPLETE",
                            "VARIABLE_FLAGS",
                        ];
                        let flags = order
                            .iter()
                            .filter(|flag| s.flags.iter().any(|f| f == *flag))
                            .map(|flag| {
                                Reply::OwnedSimple(if flag.len() == 2 {
                                    flag.to_string()
                                } else {
                                    flag.to_ascii_lowercase()
                                })
                            })
                            .collect();
                        fields.push(field("flags", Reply::Set(flags)));
                        fields.push(field("begin_search", search(&s.begin_search)));
                        fields.push(field("find_keys", search(&s.find_keys)));
                        Reply::Map(fields)
                    })
                    .collect()
            })
            .unwrap_or_default(),
    )
}
fn search(data: &HashMap<String, serde_json::Value>) -> Reply {
    let (kind, values) = data.iter().next().expect("one generated search variant");
    let fields: &[(&str, &str)] = match kind.as_str() {
        "index" => &[("pos", "index")],
        "keyword" => &[("keyword", "keyword"), ("startfrom", "startfrom")],
        "range" => &[
            ("lastkey", "lastkey"),
            ("step", "keystep"),
            ("limit", "limit"),
        ],
        "keynum" => &[
            ("keynumidx", "keynumidx"),
            ("firstkey", "firstkey"),
            ("step", "keystep"),
        ],
        "unknown" => &[],
        _ => unreachable!("generated search variant"),
    };
    let spec = fields
        .iter()
        .map(|(source, target)| {
            let value = &values[source];
            field(
                target,
                if let Some(n) = value.as_i64() {
                    Reply::Integer(n)
                } else {
                    bulk(value.as_str().expect("generated search parameter"))
                },
            )
        })
        .collect();
    Reply::Map(vec![
        field("type", bulk(kind)),
        field("spec", Reply::Map(spec)),
    ])
}

pub(super) fn header(name: &str) -> Option<(i64, [i64; 3], Reply)> {
    catalog().get(name).map(|info| {
        (
            info.arity,
            info.legacy,
            Reply::Set(info.flags.iter().cloned().map(Reply::OwnedSimple).collect()),
        )
    })
}

pub(super) fn documentation(name: &str) -> Option<&'static serde_json::Value> {
    catalog().get(name).map(|info| &info.documentation)
}
