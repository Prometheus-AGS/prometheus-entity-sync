#![no_main]

use libfuzzer_sys::fuzz_target;
use pes_protocol::decode_server;

fuzz_target!(|data: &[u8]| {
    // The only contract under test: decode_server must never panic on any
    // byte sequence, valid MessagePack or not. A Result::Err is a
    // perfectly acceptable outcome for malformed input — a panic is not.
    let _ = decode_server(data);
});
