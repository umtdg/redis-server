// ===========================================================
// ParseError, ParseErrorKind, ParseResult
// ===========================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseErrorKind {
    InvalidTag { tag: u8 },

    EmptyData,
    MissingCRLF,
    MissingData { needed: usize },
    ExtraData { extra: usize },

    InvalidData,
    InvalidUtf8Data,
    InvalidIntegerData { data: u8 },

    IntegerOverflow,

    InvalidLength { len: i64 },

    InvalidCmd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
    kind: ParseErrorKind,
}

impl ParseError {
    pub fn new(kind: ParseErrorKind) -> ParseError {
        ParseError { kind }
    }
}

pub type ParseResult<T> = Result<T, ParseError>;

// ===========================================================
// Parser
// ===========================================================

pub fn read_i64(mut data: &[u8]) -> ParseResult<i64> {
    if data.is_empty() {
        return Ok(0);
    }

    if data.len() > 20 {
        return Err(ParseError::new(ParseErrorKind::IntegerOverflow));
    }

    if data.len() == 20 && data[0] != b'+' && data[0] != b'-' {
        return Err(ParseError::new(ParseErrorKind::IntegerOverflow));
    }

    let sign = match data.first().unwrap() {
        b'+' => {
            data = &data[1..];
            1
        }
        b'-' => {
            data = &data[1..];
            -1
        }
        _ => 1,
    };

    const BASE: i64 = 10;
    const MAX_OVER_BASE: i64 = i64::MAX / BASE;
    let mut value: i64 = 0;
    for c in data.iter() {
        let digit = c.wrapping_sub(b'0');
        if digit as i64 >= BASE {
            return Err(ParseError::new(ParseErrorKind::InvalidIntegerData {
                data: *c,
            }));
        }

        if value >= MAX_OVER_BASE {
            return Err(ParseError::new(ParseErrorKind::IntegerOverflow));
        }

        value *= BASE;
        value += digit as i64;
    }

    Ok(sign * value)
}

pub fn read_str(data: &[u8]) -> ParseResult<String> {
    unsafe { Ok(String::from_utf8_unchecked(data.to_vec())) }
}

pub struct RespParser<'a> {
    pub(crate) data: &'a [u8],
}

impl<'a> RespParser<'a> {
    pub fn new(data: &'a [u8]) -> RespParser<'a> {
        RespParser { data }
    }

    fn split_line(&self) -> ParseResult<(&'a [u8], &'a [u8])> {
        for i in 0..self.data.len() - 1 {
            if self.data[i] == b'\r' && self.data[i + 1] == b'\n' {
                return Ok((&self.data[0..i], &self.data[i + 2..]));
            }
        }

        Err(ParseError::new(ParseErrorKind::MissingCRLF))
    }

    pub fn peek_first(&self) -> Option<&u8> {
        self.data.first()
    }

    pub fn read_bytes(&mut self, len: usize) -> ParseResult<&'a [u8]> {
        if len > self.data.len() {
            return Err(ParseError::new(ParseErrorKind::MissingData {
                needed: len - self.data.len(),
            }));
        }
        let (res, data) = self.data.split_at(len);
        self.data = data;
        Ok(res)
    }

    pub fn read_line(&mut self) -> ParseResult<&'a [u8]> {
        let (line, data) = self.split_line()?;
        self.data = data;
        Ok(line)
    }
}
