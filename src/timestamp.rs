use std::fmt::Display;
use std::str::FromStr;
use std::time::SystemTime;

#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn now() -> Self {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        Timestamp(ts as u64)
    }

    pub fn to_le_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

impl FromStr for Timestamp {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match u64::from_str(s) {
            Ok(timestamp) => Ok(Timestamp(timestamp)),
            Err(err) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
        }
    }
}

impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        Timestamp(value)
    }
}

impl From<Timestamp> for u64 {
    fn from(ts: Timestamp) -> u64 {
        ts.0
    }
}
