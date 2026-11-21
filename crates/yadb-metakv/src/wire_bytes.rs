//! Base64 for the byte fields that cross the wire as JSON.
//!
//! `serde`'s default encoding for `Vec<u8>` is a JSON array of decimal
//! numbers, which costs up to four bytes per byte (`255,`), so a byte field
//! written twice - once inside a message and again inside the signed envelope
//! that carries it - reaches sixteen bytes per byte. RFC 4648 base64 with
//! padding costs 4/3 bytes per byte, so the same field reaches 16/9, and it
//! decodes in one pass.
//!
//! Two properties are deliberate:
//!
//! * the decoder still accepts the legacy array of numbers, so a peer running
//!   an older encoder is understood rather than misread;
//! * it is strict about padding, alphabet and unused bits, so a damaged field
//!   is refused instead of decoding to different bytes. A receiver that
//!   silently repaired a field would turn a transport fault into a state
//!   divergence.

/// Standard alphabet with padding, as RFC 4648 section 4 defines it.
const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encodes `bytes` as padded base64.
pub(crate) fn encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for group in bytes.chunks(3) {
        let b0 = group[0] as u32;
        let b1 = *group.get(1).unwrap_or(&0) as u32;
        let b2 = *group.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(n >> 18) as usize & 0x3f] as char);
        out.push(ALPHABET[(n >> 12) as usize & 0x3f] as char);
        out.push(if group.len() > 1 {
            ALPHABET[(n >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if group.len() > 2 {
            ALPHABET[n as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

/// Strict base64 decode: the input must be padded, four characters per group,
/// with `=` only in the last two positions and no non-zero unused bits.
pub(crate) fn decode(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if bytes.len() % 4 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for (group_index, group) in bytes.chunks(4).enumerate() {
        let mut n = 0u32;
        let mut padding = 0usize;
        for (index, &byte) in group.iter().enumerate() {
            let value = if byte == b'=' {
                if index < 2 {
                    return None;
                }
                padding += 1;
                0
            } else {
                if padding != 0 {
                    return None;
                }
                value_of(byte)?
            };
            n = (n << 6) | value as u32;
        }
        // Padding is only legal in the final group.
        if padding != 0 && group_index + 1 != bytes.len() / 4 {
            return None;
        }
        match padding {
            0 => out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8, n as u8]),
            1 if n & 0xff == 0 => out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8]),
            2 if n & 0xffff == 0 => out.push((n >> 16) as u8),
            _ => return None,
        }
    }
    Some(out)
}

fn value_of(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// The upper bound a byte field can reach on the wire: base64 of the bytes,
/// plus room for the field name and the JSON around it.
pub(crate) const fn encoded_upper_bound(bytes: usize) -> usize {
    bytes.div_ceil(3) * 4
}

/// Serialises a byte field as base64. Use with `#[serde(with = "...")]`.
pub(crate) fn serialize<S: serde::Serializer>(
    bytes: &[u8],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&encode(bytes))
}

/// Deserialises a byte field from base64, still accepting the legacy array of
/// numbers. Use with `#[serde(with = "...")]`.
pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<u8>, D::Error> {
    use serde::de::{Error, SeqAccess, Visitor};

    struct WireBytes;

    impl<'de> Visitor<'de> for WireBytes {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a base64 string or an array of byte values")
        }

        fn visit_str<E: Error>(self, value: &str) -> Result<Vec<u8>, E> {
            decode(value).ok_or_else(|| E::custom("invalid base64 byte field"))
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<u8>, A::Error> {
            let mut out = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
            while let Some(byte) = sequence.next_element::<u8>()? {
                out.push(byte);
            }
            Ok(out)
        }
    }

    deserializer.deserialize_any(WireBytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_alphabet_round_trips_every_byte_and_every_padding_length() {
        let all: Vec<u8> = (0..=255u8).collect();
        for length in 0..=all.len() {
            let bytes = &all[..length];
            let text = encode(bytes);
            assert_eq!(text.len(), encoded_upper_bound(length), "length {length}");
            assert_eq!(decode(&text).as_deref(), Some(bytes), "length {length}");
        }
    }

    #[test]
    fn a_damaged_field_is_refused_rather_than_decoded_to_other_bytes() {
        // Wrong length, illegal character, padding inside the string, padding
        // in the first position, and padding in a non-final group.
        for text in [
            "A", "AB", "ABC", "AAAAA", "AB!D", "A=AA", "=AAA", "AB=C", "AA==AA==",
        ] {
            assert_eq!(decode(text), None, "{text:?} must be refused");
        }
        // The unused bits of a padded group must be zero, or two encodings
        // would name the same bytes and a corruption would go unnoticed.
        assert_eq!(decode("AB=="), None, "unused bits must be zero");
        assert_eq!(decode("AAB="), None, "unused bits must be zero");
        // The valid forms of the same lengths still decode.
        assert_eq!(decode("AA=="), Some(vec![0]));
        assert_eq!(decode("AQ=="), Some(vec![1]));
        assert_eq!(decode("AAE="), Some(vec![0, 1]));
        assert_eq!(decode(""), Some(Vec::new()));
    }
}
