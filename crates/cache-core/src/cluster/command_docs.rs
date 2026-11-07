//! Documentary fields for the supported Redis reference command subset.
use super::bulk;
use crate::protocol::Reply;
use serde_json::Value;
fn field(name: &str, value: Reply) -> (Reply, Reply) {
    (bulk(name), value)
}
fn text_fields(data: &Value, names: &[&str], fields: &mut Vec<(Reply, Reply)>) {
    for &name in names {
        if let Some(text) = data[name].as_str() {
            fields.push(field(name, bulk(text)));
        }
    }
}
pub(super) fn fields(name: &str) -> Vec<(Reply, Reply)> {
    let Some(data) = super::command_info::documentation(name) else {
        return vec![
            field(
                "summary",
                bulk("Returns Flaro write-behind persistence statistics."),
            ),
            field("group", bulk("server")),
        ];
    };
    let mut fields = Vec::new();
    text_fields(
        data,
        &["summary", "since", "group", "complexity"],
        &mut fields,
    );
    if let Some(flags) = data["doc_flags"]
        .as_array()
        .filter(|items| !items.is_empty())
    {
        fields.push(field(
            "doc_flags",
            Reply::Set(
                ["DEPRECATED", "SYSCMD"]
                    .iter()
                    .filter(|name| flags.iter().any(|f| f.as_str() == Some(name)))
                    .map(|name| Reply::OwnedSimple(name.to_ascii_lowercase()))
                    .collect(),
            ),
        ));
    }
    text_fields(data, &["deprecated_since", "replaced_by"], &mut fields);
    if let Some(history) = data["history"].as_array().filter(|items| !items.is_empty()) {
        fields.push(field(
            "history",
            Reply::Set(
                history
                    .iter()
                    .map(|entry| {
                        Reply::Array(
                            entry
                                .as_array()
                                .expect("generated history")
                                .iter()
                                .map(|value| bulk(value.as_str().expect("generated history text")))
                                .collect(),
                        )
                    })
                    .collect(),
            ),
        ));
    }
    if let Some(args) = data["arguments"]
        .as_array()
        .filter(|items| !items.is_empty())
    {
        fields.push(field("arguments", arguments(args)));
    }
    fields
}
fn arguments(args: &[Value]) -> Reply {
    Reply::Array(
        args.iter()
            .map(|arg| {
                let mut fields = Vec::new();
                text_fields(arg, &["name", "type"], &mut fields);
                let nested = matches!(arg["type"].as_str(), Some("oneof" | "block"));
                if !nested {
                    fields.push(field(
                        "display_text",
                        bulk(
                            arg["display"]
                                .as_str()
                                .or_else(|| arg["name"].as_str())
                                .expect("generated argument name"),
                        ),
                    ));
                }
                if let Some(index) = arg["key_spec_index"].as_i64() {
                    fields.push(field("key_spec_index", Reply::Integer(index)));
                }
                if let Some(token) = arg["token"].as_str() {
                    fields.push(field("token", bulk(token.to_ascii_uppercase())));
                }
                text_fields(arg, &["summary", "since", "deprecated_since"], &mut fields);
                let flags: Vec<_> = ["optional", "multiple", "multiple_token"]
                    .iter()
                    .filter(|name| arg[**name].as_bool() == Some(true))
                    .map(|flag| Reply::Simple(flag))
                    .collect();
                if !flags.is_empty() {
                    fields.push(field("flags", Reply::Set(flags)));
                }
                if nested {
                    fields.push(field(
                        "arguments",
                        arguments(
                            arg["arguments"]
                                .as_array()
                                .expect("generated nested arguments"),
                        ),
                    ));
                }
                Reply::Map(fields)
            })
            .collect(),
    )
}
