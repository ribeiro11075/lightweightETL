//! The `fake*` strategies: a realistic replacement picked from lists by the
//! keyed hash. `masking._FakeStrategy` and its subclasses.
//!
//! The lists are Python's, handed over when a masker is built, so there is one
//! copy of them to keep: changing a list changes every mask made from it, and a
//! second copy here could drift from the first without anyone noticing.
//!
//! Unlike `key` and `email`, nothing here depends on Python's view of text:
//! a value is keyed on its UTF-8 bytes as they are, so any text is masked here.

use num_bigint::BigUint;
use num_traits::ToPrimitive;

use crate::KeyedHash;

/// One locale's lists, and the company words every locale shares.
#[derive(Clone, Debug)]
pub struct FakeLists {
    pub firstNames: Vec<String>,
    pub lastNames: Vec<String>,
    pub cities: Vec<String>,
    pub streets: Vec<String>,
    pub streetKinds: Vec<String>,
    /// Python's format string, with `{number}`, `{street}` and `{kind}`.
    pub address: String,
    pub companySuffixes: Vec<String>,
    pub companyWords: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FakeKind {
    FirstName,
    LastName,
    Name,
    City,
    Company,
    StreetAddress,
}

impl FakeKind {
    /// The strategy's name as a policy spells it.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "fakeFirstName" => Some(FakeKind::FirstName),
            "fakeLastName" => Some(FakeKind::LastName),
            "fakeName" => Some(FakeKind::Name),
            "fakeCity" => Some(FakeKind::City),
            "fakeCompany" => Some(FakeKind::Company),
            "fakeStreetAddress" => Some(FakeKind::StreetAddress),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
enum Piece {
    Text(String),
    Number,
    Street,
    Kind,
}

#[derive(Clone, Debug)]
pub struct FakeStrategy {
    kind: FakeKind,
    lists: FakeLists,
    maxLength: Option<usize>,
    address: Vec<Piece>,
}

impl FakeStrategy {
    /// Refuses what it can't reproduce exactly -- an empty list, or an address
    /// format with anything but the three fields -- so Python masks instead.
    pub fn new(kind: FakeKind, lists: FakeLists, maxLength: Option<usize>) -> Result<Self, String> {
        let needed: Vec<(&str, &Vec<String>)> = match kind {
            FakeKind::FirstName => vec![("firstNames", &lists.firstNames)],
            FakeKind::LastName => vec![("lastNames", &lists.lastNames)],
            FakeKind::Name => vec![("firstNames", &lists.firstNames), ("lastNames", &lists.lastNames)],
            FakeKind::City => vec![("cities", &lists.cities)],
            FakeKind::Company => vec![("companyWords", &lists.companyWords), ("companySuffixes", &lists.companySuffixes)],
            FakeKind::StreetAddress => vec![("streets", &lists.streets), ("streetKinds", &lists.streetKinds)],
        };
        if let Some((name, _)) = needed.iter().find(|(_, list)| list.is_empty()) {
            return Err(format!("{name} is empty"));
        }

        let address = parseAddress(&lists.address)?;

        Ok(Self { kind, lists, maxLength, address })
    }

    /// The mask of a value's canonical bytes (`masking._canonical`).
    pub fn mask(&self, hash: &KeyedHash, canonical: &[u8]) -> String {
        let lists = &self.lists;
        let generated = match self.kind {
            FakeKind::FirstName => pick(hash, &lists.firstNames, canonical, b"first").to_owned(),
            FakeKind::LastName => pick(hash, &lists.lastNames, canonical, b"last").to_owned(),
            FakeKind::Name => format!("{} {}", pick(hash, &lists.firstNames, canonical, b"first"), pick(hash, &lists.lastNames, canonical, b"last")),
            FakeKind::City => pick(hash, &lists.cities, canonical, b"city").to_owned(),
            FakeKind::Company => {
                format!("{} {}", pick(hash, &lists.companyWords, canonical, b"company"), pick(hash, &lists.companySuffixes, canonical, b"suffix"))
            }
            FakeKind::StreetAddress => {
                let number = below(hash, 9999, canonical, b"number") + 1;
                let street = pick(hash, &lists.streets, canonical, b"street");
                let kind = pick(hash, &lists.streetKinds, canonical, b"suffix");
                let mut out = String::new();
                for piece in &self.address {
                    match piece {
                        Piece::Text(text) => out.push_str(text),
                        Piece::Number => out.push_str(&number.to_string()),
                        Piece::Street => out.push_str(street),
                        Piece::Kind => out.push_str(kind),
                    }
                }
                out
            }
        };

        // Python slices by character, not by byte: `generated[:maxLength]`.
        match self.maxLength {
            Some(length) if generated.chars().count() > length => generated.chars().take(length).collect(),
            _ => generated,
        }
    }
}

fn below(hash: &KeyedHash, upper: usize, message: &[u8], purpose: &[u8]) -> usize {
    hash.below(message, &BigUint::from(upper), purpose).to_usize().expect("below a usize bound")
}

fn pick<'a>(hash: &KeyedHash, choices: &'a [String], message: &[u8], purpose: &[u8]) -> &'a str {
    &choices[below(hash, choices.len(), message, purpose)]
}

/// Python's `str.format` for the address templates in use: literal text and
/// the three named fields. Anything else -- a format spec, an escaped brace --
/// is refused rather than guessed at.
fn parseAddress(template: &str) -> Result<Vec<Piece>, String> {
    let mut pieces = Vec::new();
    let mut rest = template;

    while let Some(open) = rest.find('{') {
        if rest[..open].contains('}') {
            return Err(format!("unsupported address format {template:?}"));
        }
        if open > 0 {
            pieces.push(Piece::Text(rest[..open].to_owned()));
        }
        let close = rest[open..].find('}').map(|offset| open + offset).ok_or_else(|| format!("unsupported address format {template:?}"))?;
        pieces.push(match &rest[open + 1..close] {
            "number" => Piece::Number,
            "street" => Piece::Street,
            "kind" => Piece::Kind,
            _ => return Err(format!("unsupported address format {template:?}")),
        });
        rest = &rest[close + 1..];
    }
    if rest.contains('}') {
        return Err(format!("unsupported address format {template:?}"));
    }
    if !rest.is_empty() {
        pieces.push(Piece::Text(rest.to_owned()));
    }

    Ok(pieces)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_formats_other_than_the_three_fields_are_refused() {
        assert!(parseAddress("{number} {street} {kind}").is_ok());
        assert!(parseAddress("{kind} {street}, {number}").is_ok());
        for template in ["{number:05d} {street}", "{{literal}} {street}", "{street} }", "{city}", "{street"] {
            assert!(parseAddress(template).is_err(), "{template}");
        }
    }
}
