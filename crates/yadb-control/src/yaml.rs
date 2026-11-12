use serde_json::{Map, Number, Value};

use crate::ControlError;

/// Lightweight, zero-dependency YAML subset parser tailored for Kubernetes-style manifests:
/// supports multi-document `---`, nested mappings, block sequences (`- item` & `- key: val`),
/// inline `[a, b]` arrays, inline `{k: v}` maps, numbers, booleans, quotes, and `#` comments.
pub fn parse_yaml_documents(input: &str) -> Result<Vec<Value>, ControlError> {
    let mut docs = Vec::new();
    let mut current_lines: Vec<(usize, usize, &str)> = Vec::new();

    for (line_idx, raw_line) in input.lines().enumerate() {
        let stripped = strip_comment(raw_line).trim_end();
        if stripped.trim().is_empty() {
            continue;
        }
        if stripped.trim() == "---" {
            if !current_lines.is_empty() {
                let mut pos = 0;
                docs.push(parse_block(&current_lines, &mut pos, 0)?);
                current_lines.clear();
            }
            continue;
        }
        let indent = stripped.chars().take_while(|&c| c == ' ').count();
        current_lines.push((line_idx + 1, indent, stripped.trim_start()));
    }

    if !current_lines.is_empty() {
        let mut pos = 0;
        docs.push(parse_block(&current_lines, &mut pos, 0)?);
    }

    Ok(docs)
}

fn strip_comment(s: &str) -> &str {
    let mut in_single = false;
    let mut in_double = false;
    for (idx, ch) in s.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '#' if !in_single && !in_double => return &s[..idx],
            _ => {}
        }
    }
    s
}

fn parse_block(
    lines: &[(usize, usize, &str)],
    pos: &mut usize,
    min_indent: usize,
) -> Result<Value, ControlError> {
    if *pos >= lines.len() {
        return Ok(Value::Null);
    }
    let (_, first_indent, first_text) = lines[*pos];
    if first_indent < min_indent {
        return Ok(Value::Null);
    }

    if first_text.starts_with("- ") || first_text == "-" {
        parse_seq(lines, pos, first_indent)
    } else {
        parse_map(lines, pos, first_indent)
    }
}

fn parse_seq(
    lines: &[(usize, usize, &str)],
    pos: &mut usize,
    seq_indent: usize,
) -> Result<Value, ControlError> {
    let mut items = Vec::new();
    while *pos < lines.len() {
        let (line_no, indent, text) = lines[*pos];
        if indent < seq_indent {
            break;
        }
        if indent > seq_indent {
            return Err(ControlError::YamlSyntax {
                line: line_no,
                msg: "unexpected indentation inside sequence".into(),
            });
        }
        if !(text.starts_with("- ") || text == "-") {
            break;
        }

        let after_dash = text[1..].trim_start();
        *pos += 1;

        if after_dash.is_empty() {
            items.push(parse_block(lines, pos, seq_indent + 1)?);
        } else if let Some((k, v_rest)) = split_key_value(after_dash) {
            // Inline map item starting on `- key: value` line
            let mut map = Map::new();
            let item_map_indent = indent + 2;
            if v_rest.is_empty() {
                map.insert(k.to_string(), parse_block(lines, pos, item_map_indent + 1)?);
            } else {
                map.insert(k.to_string(), parse_scalar_or_inline(v_rest, line_no)?);
            }

            while *pos < lines.len() {
                let (sub_line, sub_indent, sub_text) = lines[*pos];
                if sub_indent < item_map_indent {
                    break;
                }
                if sub_indent > item_map_indent {
                    return Err(ControlError::YamlSyntax {
                        line: sub_line,
                        msg: "invalid indentation in sequence item mapping".into(),
                    });
                }
                if sub_text.starts_with("- ") || sub_text == "-" {
                    break;
                }
                let (sk, sv) =
                    split_key_value(sub_text).ok_or_else(|| ControlError::YamlSyntax {
                        line: sub_line,
                        msg: format!("expected key: value, got `{sub_text}`"),
                    })?;
                *pos += 1;
                if sv.is_empty() {
                    map.insert(
                        sk.to_string(),
                        parse_block(lines, pos, item_map_indent + 1)?,
                    );
                } else {
                    map.insert(sk.to_string(), parse_scalar_or_inline(sv, sub_line)?);
                }
            }
            items.push(Value::Object(map));
        } else {
            items.push(parse_scalar_or_inline(after_dash, line_no)?);
        }
    }
    Ok(Value::Array(items))
}

fn parse_map(
    lines: &[(usize, usize, &str)],
    pos: &mut usize,
    map_indent: usize,
) -> Result<Value, ControlError> {
    let mut map = Map::new();
    while *pos < lines.len() {
        let (line_no, indent, text) = lines[*pos];
        if indent < map_indent {
            break;
        }
        if indent > map_indent {
            return Err(ControlError::YamlSyntax {
                line: line_no,
                msg: "unexpected indentation in mapping".into(),
            });
        }
        if text.starts_with("- ") || text == "-" {
            break;
        }
        let (key, rest) = split_key_value(text).ok_or_else(|| ControlError::YamlSyntax {
            line: line_no,
            msg: format!("expected `key: value`, got `{text}`"),
        })?;
        *pos += 1;

        if rest.is_empty() {
            let child = parse_block(lines, pos, map_indent + 1)?;
            map.insert(key.to_string(), child);
        } else {
            let val = parse_scalar_or_inline(rest, line_no)?;
            map.insert(key.to_string(), val);
        }
    }
    Ok(Value::Object(map))
}

fn split_key_value(s: &str) -> Option<(&str, &str)> {
    let mut in_quotes = false;
    for (idx, ch) in s.char_indices() {
        match ch {
            '"' | '\'' => in_quotes = !in_quotes,
            ':' if !in_quotes => {
                let after = &s[idx + 1..];
                if after.is_empty() || after.starts_with(' ') {
                    return Some((s[..idx].trim(), after.trim()));
                }
            }
            _ => {}
        }
    }
    None
}

fn parse_scalar_or_inline(raw: &str, line_no: usize) -> Result<Value, ControlError> {
    let s = raw.trim();
    if s.starts_with('[') && s.ends_with(']') {
        let inner = &s[1..s.len() - 1];
        if inner.trim().is_empty() {
            return Ok(Value::Array(Vec::new()));
        }
        let parts = split_top_level_commas(inner);
        let mut arr = Vec::with_capacity(parts.len());
        for p in parts {
            arr.push(parse_scalar_or_inline(p, line_no)?);
        }
        return Ok(Value::Array(arr));
    }

    if s.starts_with('{') && s.ends_with('}') {
        let inner = &s[1..s.len() - 1];
        let mut map = Map::new();
        if !inner.trim().is_empty() {
            for entry in split_top_level_commas(inner) {
                let (k, v) = split_key_value(entry).ok_or_else(|| ControlError::YamlSyntax {
                    line: line_no,
                    msg: format!("invalid inline map entry `{entry}`"),
                })?;
                map.insert(k.to_string(), parse_scalar_or_inline(v, line_no)?);
            }
        }
        return Ok(Value::Object(map));
    }

    if (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
        || (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
    {
        return Ok(Value::String(s[1..s.len() - 1].to_string()));
    }

    match s {
        "true" => return Ok(Value::Bool(true)),
        "false" => return Ok(Value::Bool(false)),
        "null" | "~" => return Ok(Value::Null),
        _ => {}
    }

    if let Ok(u) = s.parse::<u64>() {
        return Ok(Value::Number(Number::from(u)));
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(Value::Number(Number::from(i)));
    }
    if let Ok(f) = s.parse::<f64>() {
        if let Some(n) = Number::from_f64(f) {
            return Ok(Value::Number(n));
        }
    }

    Ok(Value::String(s.to_string()))
}

fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut in_quotes = false;
    let mut start = 0usize;
    for (idx, ch) in s.char_indices() {
        match ch {
            '"' | '\'' => in_quotes = !in_quotes,
            '[' | '{' if !in_quotes => depth += 1,
            ']' | '}' if !in_quotes => depth -= 1,
            ',' if !in_quotes && depth == 0 => {
                out.push(s[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }
    out.push(s[start..].trim());
    out
}
