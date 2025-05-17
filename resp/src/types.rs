use crate::{
    parser::{ParseError, ParseErrorKind, ParseResult, RespParser},
    writer::{RespWriter, WriteBuf, WriteResult},
};

// ===========================================================
// RespReadable, SimpleRespReadable, RespWritable, SimpleRespWritable
// ===========================================================

pub trait RespReadable<'a>: Sized {
    fn parse(parser: &mut RespParser<'a>) -> ParseResult<Self>;

    fn can_parse(tag: u8) -> bool;
}

pub trait SimpleRespReadable<'a>: Sized {
    const TAGS: &'a [u8];

    fn parse_raw(data: &'a [u8]) -> ParseResult<Self>;
}

impl<'a, T: SimpleRespReadable<'a>> RespReadable<'a> for T {
    fn parse(parser: &mut RespParser<'a>) -> ParseResult<Self> {
        let tag = parser.read_bytes(1)?[0];
        if !Self::can_parse(tag) {
            return Err(ParseError::new(ParseErrorKind::InvalidTag { tag }));
        }
        let data = parser.read_line()?;

        Self::parse_raw(data)
    }

    fn can_parse(tag: u8) -> bool {
        Self::TAGS.contains(&tag)
    }
}

pub trait RespWritable: Sized {
    fn write(&self, writer: &mut RespWriter<'_>) -> WriteResult;
}

pub trait SimpleRespWritable: Sized {
    const TAG: u8;

    fn write_raw(&self, buf: &mut WriteBuf) -> WriteResult;
}

impl<T> RespWritable for T
where
    T: SimpleRespWritable,
{
    fn write(&self, writer: &mut RespWriter<'_>) -> WriteResult {
        writer.write_u8(Self::TAG)?;
        self.write_raw(writer.buffer())?;
        writer.write_crlf()?;

        Ok(())
    }
}

// ===========================================================
// String
// ===========================================================

// TODO: Use tuple structs to differentiate Simple and Error strings

impl<'a> SimpleRespReadable<'a> for String {
    const TAGS: &'a [u8] = b"+-";

    fn parse_raw(data: &'a [u8]) -> ParseResult<Self> {
        crate::parser::read_str(data)
    }
}

impl SimpleRespWritable for String {
    const TAG: u8 = b'+';

    fn write_raw(&self, buf: &mut WriteBuf) -> WriteResult {
        buf.push_bytes(self.as_bytes())
    }
}

// ===========================================================
// Integer
// ===========================================================

// TODO: Use tuple structs to differentiate Length from Integers

impl<'a> SimpleRespReadable<'a> for i64 {
    const TAGS: &'a [u8] = b":*$";

    fn parse_raw(data: &'a [u8]) -> ParseResult<Self> {
        crate::parser::read_i64(data)
    }
}

impl SimpleRespWritable for i64 {
    const TAG: u8 = b':';

    fn write_raw(&self, buf: &mut WriteBuf) -> WriteResult {
        self.to_string().write_raw(buf)
    }
}

// ===========================================================
// BulkString
// ===========================================================

// TODO: This is NOT a String, but rather byte array

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkString(String);

impl BulkString {
    pub fn new(s: String) -> BulkString {
        BulkString(s)
    }

    pub fn value(&self) -> &String {
        &self.0
    }

    pub fn value_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

impl<'a> RespReadable<'a> for BulkString {
    fn parse(parser: &mut RespParser<'a>) -> ParseResult<Self> {
        match parser.peek_first() {
            None => return Err(ParseError::new(ParseErrorKind::EmptyData)),
            Some(tag) => {
                if !Self::can_parse(*tag) {
                    return Err(ParseError::new(ParseErrorKind::InvalidTag { tag: *tag }));
                }
            }
        }

        // Read length of the string
        let length = i64::parse(parser)?;
        if length < 0 {
            return Err(ParseError::new(ParseErrorKind::InvalidLength {
                len: length,
            }));
        }
        // TODO: Check for max length
        let length = length as usize;

        // Read the next line
        let line = parser.read_line()?;
        let line_len = line.len();
        if line_len < length {
            return Err(ParseError::new(ParseErrorKind::MissingData {
                needed: length - line_len,
            }));
        }

        // Truncate extra data
        let s = crate::parser::read_str(&line[..length])?;
        Ok(BulkString(s))
    }

    fn can_parse(tag: u8) -> bool {
        tag == b'$'
    }
}

impl RespWritable for BulkString {
    fn write(&self, writer: &mut RespWriter<'_>) -> WriteResult {
        // Tag + length
        writer.write_u8(b'$')?;
        (self.0.len() as i64).write_raw(writer.buffer())?;
        writer.write_crlf()?;

        // Value
        self.0.write_raw(writer.buffer())?;
        writer.write_crlf()?;

        Ok(())
    }
}

// ===========================================================
// Array
// ===========================================================

impl RespReadable<'_> for Vec<RespValue> {
    fn parse(parser: &mut RespParser<'_>) -> ParseResult<Self> {
        let len = i64::parse(parser)?;
        if len < 0 {
            return Err(ParseError::new(ParseErrorKind::InvalidLength { len }));
        }

        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let value = RespValue::parse(parser)?;
            vec.push(value);
        }

        Ok(vec)
    }

    fn can_parse(tag: u8) -> bool {
        tag == b'*'
    }
}

impl RespWritable for Vec<RespValue> {
    fn write(&self, writer: &mut RespWriter<'_>) -> WriteResult {
        writer.write_u8(b'*')?;
        self.len().to_string().write_raw(writer.buffer())?;
        writer.write_crlf()?;

        for value in self.iter() {
            value.write(writer)?;
        }

        Ok(())
    }
}

impl RespReadable<'_> for Vec<BulkString> {
    fn parse(parser: &mut RespParser<'_>) -> ParseResult<Self> {
        let len = i64::parse(parser)?;
        if len < 0 {
            return Err(ParseError::new(ParseErrorKind::InvalidLength { len }));
        }

        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let value = BulkString::parse(parser)?;
            vec.push(value);
        }

        Ok(vec)
    }

    fn can_parse(tag: u8) -> bool {
        tag == b'*'
    }
}

// ===========================================================
// RespValue
// ===========================================================

#[derive(Debug, PartialEq, Eq)]
pub enum RespValue {
    /// Special case of $-1\r\n
    None,

    /// Simple String starting with `+`
    Simple(String),

    /// Error String starting with `-`
    Error(String),

    /// Signed 64 Bit Integer starting with `:`
    Integer(i64),

    /// Bulk String starting with `$`
    Bulk(BulkString),

    /// Array of Values starting with `*`
    Array(Vec<RespValue>),
}

impl RespReadable<'_> for RespValue {
    fn parse(parser: &mut RespParser<'_>) -> ParseResult<Self> {
        match parser.peek_first() {
            Some(b'+') => Ok(RespValue::Simple(String::parse(parser)?)),
            Some(b'-') => Ok(RespValue::Error(String::parse(parser)?)),
            Some(b':') => Ok(RespValue::Integer(i64::parse(parser)?)),
            Some(b'$') => Ok(RespValue::Bulk(BulkString::parse(parser)?)),
            Some(b'*') => Ok(RespValue::Array(Vec::<RespValue>::parse(parser)?)),
            Some(tag) => Err(ParseError::new(ParseErrorKind::InvalidTag { tag: *tag })),
            _ => Err(ParseError::new(ParseErrorKind::EmptyData)),
        }
    }

    fn can_parse(_tag: u8) -> bool {
        true
    }
}

impl RespWritable for RespValue {
    fn write(&self, writer: &mut RespWriter<'_>) -> WriteResult {
        match self {
            RespValue::None => {
                writer.write_u8(b'$')?;
                writer.write_u8(b'-')?;
                writer.write_u8(b'1')?;
                writer.write_crlf()?;

                Ok(())
            }
            RespValue::Simple(s) => Ok(s.write(writer)?),
            RespValue::Error(e) => {
                writer.write_u8(b'-')?;
                e.write_raw(writer.buffer())?;
                writer.write_crlf()?;

                Ok(())
            }
            RespValue::Integer(i) => Ok(i.write(writer)?),
            RespValue::Bulk(bulk_string) => Ok(bulk_string.write(writer)?),
            RespValue::Array(resp_values) => Ok(resp_values.write(writer)?),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_i64() {
        let inputs = [
            b":5\r\n".to_vec(),
            b":1234\r\n".to_vec(),
            b":32345678\r\n".to_vec(),
            b":48713467133413751634\r\n".to_vec(),
            b":48a71346713a\r\n".to_vec(),
            b":48a\r\n".to_vec(),
            b":48a".to_vec(),
            b"48\r\n".to_vec(),
            b"1234".to_vec(),
            b":-5712346\r\n".to_vec(),
            b":-1234567890123456789\r\n".to_vec(),
            b":1234567890123456789\r\n".to_vec(),
            b":12345678901234567890\r\n".to_vec(),
            b":+1234567890123456789\r\n".to_vec(),
            b":+12345678901234567890\r\n".to_vec(),
            b":-12345678901234567890\r\n".to_vec(),
        ];
        let expects: &[ParseResult<i64>] = &[
            Ok(5),
            Ok(1234),
            Ok(32345678),
            Err(ParseError::new(ParseErrorKind::IntegerOverflow)),
            Err(ParseError::new(ParseErrorKind::InvalidIntegerData {
                data: b'a',
            })),
            Err(ParseError::new(ParseErrorKind::InvalidIntegerData {
                data: b'a',
            })),
            Err(ParseError::new(ParseErrorKind::MissingCRLF)),
            Err(ParseError::new(ParseErrorKind::InvalidTag { tag: b'4' })),
            Err(ParseError::new(ParseErrorKind::InvalidTag { tag: b'1' })),
            Ok(-5712346),
            Ok(-1234567890123456789),
            Ok(1234567890123456789),
            Err(ParseError::new(ParseErrorKind::IntegerOverflow)),
            Ok(1234567890123456789),
            Err(ParseError::new(ParseErrorKind::IntegerOverflow)),
            Err(ParseError::new(ParseErrorKind::IntegerOverflow)),
        ];

        assert_eq!(inputs.len(), expects.len());
        for i in 0..inputs.len() {
            let mut parser = RespParser::new(inputs[i].as_slice());
            let val = i64::parse(&mut parser);
            assert_eq!(val, expects[i]);
        }
    }

    #[test]
    fn test_parse_simple() {
        let inputs = [
            b"+This is a simple string\r\n".to_vec(),
            b"+This is another simple string that is longer than other simple string\r\n".to_vec(),
            b"+GET\r\n".to_vec(),
            b"Unknown tag\r\n".to_vec(),
            b"-Unknown tag\r\n".to_vec(),
            b"+Incomplete data".to_vec(),
            b"+Incomplete data\r".to_vec(),
        ];
        let expects: &[ParseResult<String>] = &[
            Ok("This is a simple string".to_string()),
            Ok("This is another simple string that is longer than other simple string".to_string()),
            Ok("GET".to_string()),
            Err(ParseError::new(ParseErrorKind::InvalidTag { tag: b'U' })),
            Ok("Unknown tag".to_string()),
            Err(ParseError::new(ParseErrorKind::MissingCRLF)),
            Err(ParseError::new(ParseErrorKind::MissingCRLF)),
        ];

        assert_eq!(inputs.len(), expects.len());
        for i in 0..inputs.len() {
            let mut parser = RespParser::new(&inputs[i]);
            let val = String::parse(&mut parser);
            assert_eq!(val, expects[i]);
        }
    }

    #[test]
    fn test_parse_error() {
        let inputs = [
            b"-This is a simple string\r\n".to_vec(),
            b"-This is another simple string that is longer than other simple string\r\n".to_vec(),
            b"-GET\r\n".to_vec(),
            b"Unknown tag\r\n".to_vec(),
            b"+Unknown tag\r\n".to_vec(),
            b"-Incomplete data".to_vec(),
            b"-Incomplete data\r".to_vec(),
        ];
        let expects: &[ParseResult<String>] = &[
            Ok("This is a simple string".to_string()),
            Ok("This is another simple string that is longer than other simple string".to_string()),
            Ok("GET".to_string()),
            Err(ParseError::new(ParseErrorKind::InvalidTag { tag: b'U' })),
            Ok("Unknown tag".to_string()),
            Err(ParseError::new(ParseErrorKind::MissingCRLF)),
            Err(ParseError::new(ParseErrorKind::MissingCRLF)),
        ];

        assert_eq!(inputs.len(), expects.len());
        for i in 0..inputs.len() {
            let mut parser = RespParser::new(&inputs[i]);
            let val = String::parse(&mut parser);
            assert_eq!(val, expects[i]);
        }
    }

    #[test]
    fn test_parse_bulk() {
        let inputs = [
            b"$12\r\nHello, World\r\n".to_vec(),
            b"$3\r\nGET\r\n".to_vec(),
            b"$25\r\nAAAAAAAAAAAAAAAAAAAAAAAAA\r\n".to_vec(),
            b"*3\r\nGET\r\n".to_vec(),
            b"3\r\nGET\r\n".to_vec(),
            b"$3\r\nGE\r\n".to_vec(),
            b"$3\r\nGET2\r\n".to_vec(),
            b"$3\r\nGET".to_vec(),
            b"$3GET\r\n".to_vec(),
            b"$-1\r\n".to_vec(),
            b"$-1234\r\n".to_vec(),
        ];
        let expects: &[ParseResult<BulkString>] = &[
            Ok(BulkString("Hello, World".to_string())),
            Ok(BulkString("GET".to_string())),
            Ok(BulkString("AAAAAAAAAAAAAAAAAAAAAAAAA".to_string())),
            Err(ParseError::new(ParseErrorKind::InvalidTag { tag: b'*' })),
            Err(ParseError::new(ParseErrorKind::InvalidTag { tag: b'3' })),
            Err(ParseError::new(ParseErrorKind::MissingData { needed: 1 })),
            Ok(BulkString("GET".to_string())),
            Err(ParseError::new(ParseErrorKind::MissingCRLF)),
            Err(ParseError::new(ParseErrorKind::InvalidIntegerData {
                data: b'G',
            })),
            Err(ParseError::new(ParseErrorKind::InvalidLength { len: -1 })),
            Err(ParseError::new(ParseErrorKind::InvalidLength {
                len: -1234,
            })),
        ];

        assert_eq!(inputs.len(), expects.len());
        for i in 0..inputs.len() {
            let mut parser = RespParser::new(&inputs[i]);
            let val = BulkString::parse(&mut parser);
            assert_eq!(val, expects[i]);
        }
    }

    #[test]
    fn test_parse_array() {
        let inputs = [
            b"*-1\r\n".to_vec(),
            b"*1\r\n+Simple\r\n".to_vec(),
            b"*1\r\n:-7364\r\n".to_vec(),
            b"*1\r\n-Error\r\n".to_vec(),
            b"*2\r\n$3\r\nGET\r\nkey\r\n".to_vec(),
            b"*2\r\n$3\r\nGET_SomeExtraData\r\n$3\r\nkey\r\n".to_vec(),
            b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue_someextradata\r\n".to_vec(),
            b"*3\r\n$3$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue_someextradata\r\n".to_vec(),
            b"*2\r\n$3\r\nGET_SomeExtraDatakey\r\n".to_vec(),
        ];
        let expects: &[ParseResult<RespValue>] = &[
            Err(ParseError::new(ParseErrorKind::InvalidLength { len: -1 })),
            Ok(RespValue::Array(vec![RespValue::Simple(
                "Simple".to_string(),
            )])),
            Ok(RespValue::Array(vec![RespValue::Integer(-7364)])),
            Ok(RespValue::Array(vec![RespValue::Error(
                "Error".to_string(),
            )])),
            Err(ParseError::new(ParseErrorKind::InvalidTag { tag: b'k' })),
            Ok(RespValue::Array(vec![
                RespValue::Bulk(BulkString("GET".to_string())),
                RespValue::Bulk(BulkString("key".to_string())),
            ])),
            Ok(RespValue::Array(vec![
                RespValue::Bulk(BulkString("set".to_string())),
                RespValue::Bulk(BulkString("key".to_string())),
                RespValue::Bulk(BulkString("value".to_string())),
            ])),
            Err(ParseError::new(ParseErrorKind::InvalidIntegerData {
                data: b'$',
            })),
            Err(ParseError::new(ParseErrorKind::EmptyData)),
        ];

        assert_eq!(inputs.len(), expects.len());
        for i in 0..inputs.len() {
            println!("Case {}", i + 1);
            let mut parser = RespParser::new(&inputs[i]);
            let val = RespValue::parse(&mut parser);
            assert_eq!(val, expects[i]);
        }
    }
}
