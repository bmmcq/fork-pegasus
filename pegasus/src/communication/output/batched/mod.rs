pub enum Rectifier {
    And(u64),
    Mod(u64),
}

impl Rectifier {
    pub fn new(length: usize) -> Self {
        if length & (length - 1) == 0 {
            Rectifier::And(length as u64 - 1)
        } else {
            Rectifier::Mod(length as u64)
        }
    }

    #[inline(always)]
    pub fn get(&self, v: u64) -> usize {
        let r = match self {
            Rectifier::And(b) => v & *b,
            Rectifier::Mod(b) => v % *b,
        };
        r as usize
    }
}

mod event;
mod aggregate;
mod broadcast;
