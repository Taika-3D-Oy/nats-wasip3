//! NKey authentication using Ed25519.
//!
//! NATS NKeys are Ed25519 keypairs encoded with a custom base32 scheme.
//! During authentication, the server sends a `nonce` in the INFO message.
//! The client signs the nonce with its private key and sends `sig` + `nkey`
//! (public key) in the CONNECT options.

use ed25519_dalek::{Signer, SigningKey};

use crate::Error;

const PREFIX_SEED: u8 = 18 << 3; // 'S'
const _PREFIX_USER: u8 = 20 << 3; // 'U'
const _PREFIX_ACCOUNT: u8 = 0 << 3; // 'A'
const _PREFIX_SERVER: u8 = 13 << 3; // 'N'

/// An NKey keypair for authentication.
pub struct KeyPair {
    seed_prefix: u8,
    signing: SigningKey,
}

impl KeyPair {
    /// Parse an NKey seed string (starts with `S`).
    pub fn from_seed(seed: &str) -> Result<Self, Error> {
        let decoded = decode_raw(seed)?;
        if decoded.len() < 34 {
            return Err(Error::Protocol("nkey seed too short".into()));
        }

        let prefix1 = decoded[0];
        let prefix2 = decoded[1];

        if prefix1 != (PREFIX_SEED | (prefix2 >> 5)) {
            return Err(Error::Protocol("not a valid nkey seed".into()));
        }

        // Verify CRC-16 checksum.
        let payload = &decoded[..decoded.len() - 2];
        let expected_crc =
            u16::from_le_bytes([decoded[decoded.len() - 2], decoded[decoded.len() - 1]]);
        let actual_crc = crc16(payload);
        if expected_crc != actual_crc {
            return Err(Error::Protocol("nkey checksum mismatch".into()));
        }

        // Bytes 2..34 are the 32-byte Ed25519 private seed.
        let seed_bytes: [u8; 32] = decoded[2..34]
            .try_into()
            .map_err(|_| Error::Protocol("nkey seed wrong length".into()))?;

        Ok(KeyPair {
            seed_prefix: prefix2 & 0xF8,
            signing: SigningKey::from_bytes(&seed_bytes),
        })
    }

    /// Get the public key as an NKey-encoded string.
    pub fn public_key(&self) -> String {
        let verifying = self.signing.verifying_key();
        let mut raw = Vec::with_capacity(35);
        raw.push(self.seed_prefix);
        raw.extend_from_slice(verifying.as_bytes());
        let crc = crc16(&raw);
        raw.extend_from_slice(&crc.to_le_bytes());
        encode_raw(&raw)
    }

    /// Sign a nonce for NATS authentication.
    pub fn sign(&self, nonce: &[u8]) -> String {
        let sig = self.signing.sign(nonce);
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig.to_bytes())
    }
}

// ── Base32 encoding (NATS NKey variant) ────────────────────────────

fn encode_raw(data: &[u8]) -> String {
    data_encoding::BASE32_NOPAD.encode(data)
}

fn decode_raw(s: &str) -> Result<Vec<u8>, Error> {
    data_encoding::BASE32_NOPAD
        .decode(s.as_bytes())
        .map_err(|e| Error::Protocol(format!("nkey base32 decode: {e}")))
}

// ── CRC-16 (NATS uses CRC-16/AUG-CCITT) ───────────────────────────

fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc = ((crc >> 8) & 0xFF) ^ CRC16_TABLE[((crc ^ byte as u16) & 0xFF) as usize];
    }
    crc
}

#[rustfmt::skip]
const CRC16_TABLE: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6, 0x70E7,
    0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD, 0xE1CE, 0xF1EF,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52B5, 0x4294, 0x72F7, 0x62D6,
    0x9339, 0x8318, 0xB37B, 0xA35A, 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64E6, 0x74C7, 0x44A4, 0x5485,
    0xA56A, 0xB54B, 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76D7, 0x66F6, 0x5695, 0x46B4,
    0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D, 0xC7BC,
    0x4864, 0x5845, 0x6826, 0x7807, 0x08E0, 0x18C1, 0x28A2, 0x38A3,
    0xC94C, 0xD96D, 0xE90E, 0xF92F, 0x89C8, 0x99E9, 0xA98A, 0xB9AB,
    0x5A75, 0x4A54, 0x7A37, 0x6A16, 0x1AF1, 0x0AD0, 0x3AB3, 0x2A92,
    0xDB7D, 0xCB5C, 0xFB3F, 0xEB1E, 0x9BF9, 0x8BD8, 0xBBBB, 0xAB9A,
    0x6CA6, 0x7C87, 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0x0C60, 0x1C41,
    0xEDAE, 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49,
    0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13, 0x2E32, 0x1E51, 0x0E70,
    0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A, 0x9F59, 0x8F78,
    0x9188, 0x81A9, 0xB1CA, 0xA1EB, 0xD10C, 0xC12D, 0xF14E, 0xE16F,
    0x1080, 0x00A1, 0x30C2, 0x20E3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83B9, 0x9398, 0xA3FB, 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E,
    0x02B1, 0x1290, 0x22F3, 0x32D2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xB5EA, 0xA5CB, 0x95A8, 0x8589, 0xF56E, 0xE54F, 0xD52C, 0xC50D,
    0x34E2, 0x24C3, 0x14A0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xA7DB, 0xB7FA, 0x8799, 0x97B8, 0xE75F, 0xF77E, 0xC71D, 0xD73C,
    0x26D3, 0x36F2, 0x0691, 0x16B0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xD94C, 0xC96D, 0xF90E, 0xE92F, 0x99C8, 0x89E9, 0xB98A, 0xA9AB,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18C0, 0x08E1, 0x3882, 0x28A3,
    0xCB7D, 0xDB5C, 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A,
    0x4A75, 0x5A54, 0x6A37, 0x7A16, 0x0AF1, 0x1AD0, 0x2AB3, 0x3A92,
    0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8, 0x8DC9,
    0x7C26, 0x6C07, 0x5C64, 0x4C45, 0x3CA2, 0x2C83, 0x1CE0, 0x0CC1,
    0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B, 0xBFBA, 0x8FD9, 0x9FF8,
    0x6E17, 0x7E36, 0x4E55, 0x5E74, 0x2E93, 0x3EB2, 0x0ED1, 0x1EF0,
];

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a valid NKey user seed from 32 raw bytes.
    fn make_user_seed(key_bytes: &[u8; 32]) -> String {
        let prefix2 = _PREFIX_USER; // 'U'
        let prefix1 = PREFIX_SEED | (prefix2 >> 5);
        let mut raw = Vec::with_capacity(36);
        raw.push(prefix1);
        raw.push(prefix2);
        raw.extend_from_slice(key_bytes);
        let crc = crc16(&raw);
        raw.extend_from_slice(&crc.to_le_bytes());
        encode_raw(&raw)
    }

    #[test]
    fn from_seed_and_public_key() {
        let seed_bytes = [1u8; 32];
        let seed = make_user_seed(&seed_bytes);
        assert!(seed.starts_with('S'));

        let kp = KeyPair::from_seed(&seed).unwrap();
        let pk = kp.public_key();
        // Public key should start with 'U' (user prefix)
        assert!(pk.starts_with('U'), "expected U prefix, got {pk}");
        // Deterministic — same seed -> same public key
        let kp2 = KeyPair::from_seed(&seed).unwrap();
        assert_eq!(kp2.public_key(), pk);
    }

    #[test]
    fn sign_produces_valid_base64url() {
        let seed = make_user_seed(&[42u8; 32]);
        let kp = KeyPair::from_seed(&seed).unwrap();
        let sig = kp.sign(b"test-nonce");
        // base64url-no-pad: only alphanumeric, '-', '_'
        assert!(
            sig.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "signature contains invalid chars: {sig}"
        );
        // Ed25519 signature is 64 bytes → 86 base64 chars (no pad)
        assert_eq!(sig.len(), 86, "unexpected sig length: {}", sig.len());
    }

    #[test]
    fn sign_different_nonces_differ() {
        let seed = make_user_seed(&[7u8; 32]);
        let kp = KeyPair::from_seed(&seed).unwrap();
        let sig1 = kp.sign(b"nonce-a");
        let sig2 = kp.sign(b"nonce-b");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn from_seed_too_short() {
        assert!(KeyPair::from_seed("AAAA").is_err());
    }

    #[test]
    fn from_seed_bad_prefix() {
        // Valid base32 but not a seed prefix
        let bad = encode_raw(&vec![0u8; 36]);
        assert!(KeyPair::from_seed(&bad).is_err());
    }

    #[test]
    fn from_seed_bad_checksum() {
        let seed_bytes = [1u8; 32];
        let prefix2 = _PREFIX_USER;
        let prefix1 = PREFIX_SEED | (prefix2 >> 5);
        let mut raw = Vec::with_capacity(36);
        raw.push(prefix1);
        raw.push(prefix2);
        raw.extend_from_slice(&seed_bytes);
        // Append wrong CRC
        raw.push(0xFF);
        raw.push(0xFF);
        let bad_seed = encode_raw(&raw);
        assert!(KeyPair::from_seed(&bad_seed).is_err());
    }

    #[test]
    fn from_seed_invalid_base32() {
        assert!(KeyPair::from_seed("not-valid-base32!!!").is_err());
    }

    #[test]
    fn crc16_known_value() {
        // Empty input should be 0
        assert_eq!(crc16(b""), 0);
        // Non-empty deterministic check
        let c1 = crc16(b"hello");
        let c2 = crc16(b"hello");
        assert_eq!(c1, c2);
        assert_ne!(crc16(b"hello"), crc16(b"world"));
    }
}
