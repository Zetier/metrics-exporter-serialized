use flate2::{Compression as DeflateCompression, Decompress, FlushDecompress, Status};

use crate::{ErrorKind, proto::CompressionScheme};

/// Compression for serialized metric events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    /// Emits protobuf bytes without compression.
    None,
    /// Emits raw DEFLATE.
    Deflate {
        /// Compression level.
        level: DeflateLevel,
    },
}

impl Default for Compression {
    fn default() -> Self {
        Self::Deflate {
            level: DeflateLevel::L6,
        }
    }
}

/// A DEFLATE compression level from 0 through 9.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeflateLevel {
    /// Stores data without compression, retaining DEFLATE framing.
    L0 = 0,
    /// Compression level 1.
    L1 = 1,
    /// Compression level 2.
    L2 = 2,
    /// Compression level 3.
    L3 = 3,
    /// Compression level 4.
    L4 = 4,
    /// Compression level 5.
    L5 = 5,
    /// Compression level 6.
    L6 = 6,
    /// Compression level 7.
    L7 = 7,
    /// Compression level 8.
    L8 = 8,
    /// Compression level 9.
    L9 = 9,
}

impl From<DeflateLevel> for DeflateCompression {
    fn from(level: DeflateLevel) -> Self {
        Self::new(level as u32)
    }
}

impl From<Compression> for CompressionScheme {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::None => Self::Uncompressed,
            Compression::Deflate { .. } => Self::Deflate,
        }
    }
}

pub enum Decompressor {
    Uncompressed,
    Deflate(Decompress),
}

impl TryFrom<CompressionScheme> for Decompressor {
    type Error = ErrorKind;

    fn try_from(scheme: CompressionScheme) -> Result<Self, Self::Error> {
        match scheme {
            CompressionScheme::Uncompressed => Ok(Self::Uncompressed),
            CompressionScheme::Deflate => Ok(Self::Deflate(Decompress::new(false))),
            CompressionScheme::CompressionUnspecified => {
                Err(ErrorKind::UnsupportedCompression(scheme as i32))
            }
        }
    }
}

pub struct DecompressionProgress {
    pub consumed: usize,
    pub produced: usize,
    pub finished: bool,
}

impl Decompressor {
    pub fn decompress(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        end_of_input: bool,
    ) -> Result<DecompressionProgress, ErrorKind> {
        match self {
            Self::Uncompressed => {
                let copied = input.len().min(output.len());
                output[..copied].copy_from_slice(&input[..copied]);
                Ok(DecompressionProgress {
                    consumed: copied,
                    produced: copied,
                    finished: end_of_input && copied == input.len(),
                })
            }
            Self::Deflate(inflate) => {
                let before_in = inflate.total_in();
                let before_out = inflate.total_out();
                let status = inflate
                    .decompress(input, output, FlushDecompress::None)
                    .map_err(|_| ErrorKind::InvalidEnvelope)?;
                Ok(DecompressionProgress {
                    consumed: (inflate.total_in() - before_in) as usize,
                    produced: (inflate.total_out() - before_out) as usize,
                    finished: status == Status::StreamEnd,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::DeflateEncoder;
    use std::io::Write;

    #[test]
    fn deflate_drains_output_after_consuming_all_input() {
        let raw = [b'x'; 8192];
        let mut encoder = DeflateEncoder::new(Vec::new(), DeflateCompression::default());
        encoder.write_all(&raw).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut decompressor = Decompressor::try_from(CompressionScheme::Deflate).unwrap();
        let mut output = [0; 8192];

        let first = decompressor
            .decompress(&compressed, &mut output[..1], true)
            .unwrap();
        assert_eq!(first.consumed, compressed.len());
        assert_eq!(first.produced, 1);
        assert!(!first.finished);

        let last = decompressor
            .decompress(&[], &mut output[1..], true)
            .unwrap();
        assert_eq!(last.consumed, 0);
        assert_eq!(last.produced, raw.len() - 1);
        assert!(last.finished);
        assert_eq!(output, raw);
    }
}
