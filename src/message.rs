// Automatically generated rust module for 'message.proto' file

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_imports)]
#![allow(unknown_lints)]
#![allow(clippy)]
#![cfg_attr(rustfmt, rustfmt_skip)]


use std::io::Write;
use quick_protobuf::{MessageRead, MessageWrite, BytesReader, Writer, Result};
use quick_protobuf::sizeofs::*;
use super::*;

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SubMessage {
    pub ids: Vec<i32>,
    pub subs: Vec<i32>,
}

impl<'a> MessageRead<'a> for SubMessage {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.ids = r.read_packed(bytes, |r, bytes| Ok(r.read_int32(bytes)?))?,
                Ok(18) => msg.subs = r.read_packed(bytes, |r, bytes| Ok(r.read_int32(bytes)?))?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for SubMessage {
    fn get_size(&self) -> usize {
        0
        + if self.ids.is_empty() { 0 } else { 1 + sizeof_len(self.ids.iter().map(|s| sizeof_varint(*(s) as u64)).sum::<usize>()) }
        + if self.subs.is_empty() { 0 } else { 1 + sizeof_len(self.subs.iter().map(|s| sizeof_varint(*(s) as u64)).sum::<usize>()) }
    }

    fn write_message<W: Write>(&self, w: &mut Writer<W>) -> Result<()> {
        w.write_packed_with_tag(10, &self.ids, |w, m| w.write_int32(*m), &|m| sizeof_varint(*(m) as u64))?;
        w.write_packed_with_tag(18, &self.subs, |w, m| w.write_int32(*m), &|m| sizeof_varint(*(m) as u64))?;
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct Ack {
    pub ok: bool,
}

impl<'a> MessageRead<'a> for Ack {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.ok = r.read_bool(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for Ack {
    fn get_size(&self) -> usize {
        0
        + if self.ok == false { 0 } else { 1 + sizeof_varint(*(&self.ok) as u64) }
    }

    fn write_message<W: Write>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.ok != false { w.write_with_tag(8, |w| w.write_bool(*&self.ok))?; }
        Ok(())
    }
}

