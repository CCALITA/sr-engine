# Type Encoding Specification

## Overview
Type descriptors are serialized into a canonical byte stream before hashing. The encoder writes fixed-width scalars in little-endian order and encodes strings as length-prefixed UTF-8.

## Primitive (kind 0x01, version 0x01)
Encoding layout:
- u8 kind (0x01)
- u8 version (0x01)
- u32 name_length (little-endian, byte length)
- bytes name_utf8 (no null terminator)

## Notes
- The primitive encoding is produced by `encode_primitive` in `src/engine/type_encoding.cpp`.
- The encoder writes raw bytes from the input string; callers must supply valid UTF-8 if required.
- `name_length` is the byte length of the UTF-8 sequence and must fit in `u32`; oversized inputs throw `std::length_error`.
- The TypeId is derived from the first 8 bytes of the type digest interpreted as little-endian, while the full 16-byte digest is stored as the fingerprint for collision detection.
