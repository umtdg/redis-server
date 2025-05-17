use crate::types::RespWritable;

// ===========================================================
// WriteErrorKind, WriteError, WriteResult
// ===========================================================

#[derive(Debug)]
pub enum WriteError {
    AllocationError,
}

pub type WriteResult<T = ()> = Result<T, WriteError>;

// ===========================================================
// WriteBuf, RespWriter
// ===========================================================

pub struct WriteBuf(Vec<u8>);

impl WriteBuf {
    pub fn new(data: Vec<u8>) -> WriteBuf {
        WriteBuf(data)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self) -> &Vec<u8> {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }

    pub fn push_u8(&mut self, b: u8) -> WriteResult {
        self.0
            .try_reserve(1)
            .map_err(|_| WriteError::AllocationError)?;
        self.0.push(b);
        Ok(())
    }

    pub fn push_bytes(&mut self, data: &[u8]) -> WriteResult {
        self.0
            .try_reserve(data.len())
            .map_err(|_| WriteError::AllocationError)?;
        self.0.extend_from_slice(data);
        Ok(())
    }
}

pub struct RespWriter<'a> {
    buf: &'a mut WriteBuf,
}

impl RespWriter<'_> {
    pub fn new(buf: &mut WriteBuf) -> RespWriter<'_> {
        RespWriter { buf }
    }

    pub fn buffer(&mut self) -> &mut WriteBuf {
        self.buf
    }

    pub fn write_value<T: RespWritable>(&mut self, value: &T) -> WriteResult {
        value.write(self)
    }

    pub fn write_u8(&mut self, value: u8) -> WriteResult {
        self.buf.push_u8(value)
    }

    pub fn write_crlf(&mut self) -> WriteResult {
        self.buf.push_bytes(b"\r\n")
    }
}
