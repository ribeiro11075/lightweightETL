//! The PyO3 layer: conversions in, results out, and nothing else.
//!
//! Masking itself lives in `bauta-core`, which has no Python
//! dependency. Keeping the boundary thin is what lets the constructions be
//! tested without an interpreter.
//!
//! One call per column rather than per value. The interpreter is entered once
//! for the batch, and the work between the conversions runs with the GIL
//! released -- which is what lets a masking thread overlap with a reader and a
//! writer, and, later, with other masking threads.
//!
//! Values this crate does not handle are not errors. `Decimal`, `UUID`, dates,
//! bytes and non-ASCII text come back marked, and the Python layer masks those
//! itself, in position order, so a refusal Python would raise still wins.

#![allow(non_snake_case)]

use std::collections::HashMap;

use num_bigint::BigInt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyString};

use bauta_core::cheap;
use bauta_core::{Charset, FpeStrategy, KeyStrategy, KeyedHash, MaskError};

/// Why a position came back unmasked. The Python layer turns REFUSED into
/// MaskingError with the message alongside it, and FALLBACK into a call to its
/// own implementation.
const FALLBACK: &str = "fallback";
const REFUSED: &str = "refused";

/// What crossed the boundary, owned by Rust so the GIL can be dropped.
enum Input {
    Null,
    Int(BigInt),
    Text(String),
    /// A type this crate does not mask; Python's to handle.
    Other,
    /// A bool, which every strategy here refuses -- and must, since Python
    /// checks for it before the int branch that would otherwise swallow it.
    Bool,
}

enum Output {
    Null,
    Int(BigInt),
    Text(String),
    Fallback,
    Refused(String),
}

enum Strategy {
    Key(KeyStrategy),
    Fpe(Box<FpeStrategy>),
    Hash { length: usize, prefix: String },
    Email { length: usize, mailDomain: String, keepDomain: bool },
    Digits { keepLeading: usize, keepTrailing: usize },
}

impl Strategy {
    /// The strategy's own name, for the refusal messages that carry it.
    fn name(&self) -> &'static str {
        match self {
            Strategy::Key(_) => "key",
            Strategy::Fpe(_) => "fpe",
            Strategy::Hash { .. } => "hash",
            Strategy::Email { .. } => "email",
            Strategy::Digits { .. } => "digits",
        }
    }

    fn maskOne(&self, hash: &KeyedHash, input: &Input) -> Output {
        let result: Result<Output, MaskError> = match (self, input) {
            (_, Input::Null) => return Output::Null,
            (_, Input::Other) => return Output::Fallback,

            (Strategy::Key(_) | Strategy::Fpe(_), Input::Bool) => Err(MaskError::notABool(self.name())),
            (Strategy::Digits { .. }, Input::Bool) => Err(MaskError::Refused(
                "the digits strategy needs text or an integer, got bool".to_owned(),
            )),
            // hash keys bools as 1/0 and email refuses them by type name, both
            // of which the Python layer is better placed to spell.
            (_, Input::Bool) => return Output::Fallback,

            (Strategy::Key(strategy), Input::Int(value)) => strategy.maskInteger(hash, value).map(Output::Int),
            (Strategy::Key(strategy), Input::Text(value)) => strategy.maskText(hash, value).map(Output::Text),

            (Strategy::Fpe(strategy), Input::Int(value)) => strategy.maskInteger(hash, value).map(Output::Int),
            (Strategy::Fpe(strategy), Input::Text(value)) => strategy.maskText(hash, value).map(Output::Text),

            (Strategy::Hash { length, prefix }, Input::Int(value)) => {
                Ok(Output::Text(cheap::maskHash(hash, value.to_string().as_bytes(), *length, prefix)))
            }
            (Strategy::Hash { length, prefix }, Input::Text(value)) => {
                Ok(Output::Text(cheap::maskHash(hash, value.as_bytes(), *length, prefix)))
            }

            // email takes text only, and names the offending type in its
            // refusal -- which Python spells, so an integer goes back.
            (Strategy::Email { .. }, Input::Int(_)) => return Output::Fallback,
            (Strategy::Email { length, mailDomain, keepDomain }, Input::Text(value)) => {
                cheap::maskEmail(hash, value, *length, mailDomain, *keepDomain).map(Output::Text)
            }

            (Strategy::Digits { keepLeading, keepTrailing }, Input::Int(value)) => {
                cheap::maskDigitsInteger(hash, value, *keepLeading, *keepTrailing).map(Output::Int)
            }
            (Strategy::Digits { keepLeading, keepTrailing }, Input::Text(value)) => {
                cheap::maskDigitsText(hash, value, *keepLeading, *keepTrailing).map(Output::Text)
            }
        };

        match result {
            Ok(output) => output,
            Err(MaskError::Unsupported) => Output::Fallback,
            Err(MaskError::Refused(message)) => Output::Refused(message),
        }
    }
}

/// One column's masker: the key, the domain and the strategy, built once and
/// called per chunk.
#[pyclass]
struct Masker {
    hash: KeyedHash,
    strategy: Strategy,
}

#[pymethods]
impl Masker {
    /// `subkey` is what `masking.KeyedHash` already derived for this column,
    /// so the masking key itself never crosses the boundary.
    #[new]
    #[pyo3(signature = (subkey, strategy, options))]
    fn new(subkey: &[u8], strategy: &str, options: &Bound<'_, PyDict>) -> PyResult<Self> {
        let subkey: &[u8; 32] = subkey
            .try_into()
            .map_err(|_| PyValueError::new_err("a subkey is 32 bytes"))?;
        let hash = KeyedHash::fromSubkey(subkey);

        let text = |name: &str, fallback: &str| -> PyResult<String> {
            Ok(match options.get_item(name)? {
                Some(value) => value.extract::<String>()?,
                None => fallback.to_owned(),
            })
        };
        let number = |name: &str, fallback: usize| -> PyResult<usize> {
            Ok(match options.get_item(name)? {
                Some(value) => value.extract::<usize>()?,
                None => fallback,
            })
        };
        let flag = |name: &str| -> PyResult<bool> {
            Ok(match options.get_item(name)? {
                Some(value) => value.extract::<bool>()?,
                None => false,
            })
        };

        let charset = || -> PyResult<Charset> {
            let name = text("charset", "alphanumeric")?;
            Charset::parse(&name).ok_or_else(|| PyValueError::new_err(format!("unknown charset {name:?}")))
        };

        let strategy = match strategy {
            "key" => Strategy::Key(KeyStrategy::new(charset()?)),
            "fpe" => Strategy::Fpe(Box::new(FpeStrategy::new(&hash, charset()?, flag("strict")?))),
            "hash" => Strategy::Hash { length: number("length", 16)?, prefix: text("prefix", "")? },
            "email" => Strategy::Email {
                length: number("length", 12)?,
                mailDomain: text("mailDomain", "example.test")?,
                keepDomain: flag("keepDomain")?,
            },
            "digits" => Strategy::Digits {
                keepLeading: number("keepLeading", 0)?,
                keepTrailing: number("keepTrailing", 0)?,
            },
            other => return Err(PyValueError::new_err(format!("no native masker for strategy {other:?}"))),
        };

        Ok(Self { hash, strategy })
    }

    /// Masks one column.
    ///
    /// Returns `(masked, problems)`. `problems` maps a position to FALLBACK or
    /// to a REFUSED message; every other position of `masked` is the answer.
    /// The caller walks `problems` in position order, so a refusal Python would
    /// have raised first still raises first.
    fn maskColumn<'py>(&self, py: Python<'py>, values: &Bound<'py, PyList>) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyDict>)> {
        // Convert with the GIL held.
        let mut inputs = Vec::with_capacity(values.len());
        for value in values.iter() {
            inputs.push(if value.is_none() {
                Input::Null
            } else if value.is_instance_of::<pyo3::types::PyBool>() {
                Input::Bool
            } else if value.is_instance_of::<PyString>() {
                Input::Text(value.extract::<String>()?)
            } else if value.is_instance_of::<pyo3::types::PyInt>() {
                match value.extract::<BigInt>() {
                    Ok(number) => Input::Int(number),
                    Err(_) => Input::Other,
                }
            } else {
                Input::Other
            });
        }

        // Compute without it. Repeats within a batch are masked once: a foreign
        // key or a status column is mostly repeats, and `key` and `fpe` cost
        // tens of microseconds a value.
        let outputs = py.detach(|| {
            let mut seen: HashMap<&str, usize> = HashMap::new();
            let mut outputs: Vec<Output> = Vec::with_capacity(inputs.len());

            for (index, input) in inputs.iter().enumerate() {
                if let Input::Text(text) = input {
                    if let Some(first) = seen.get(text.as_str()) {
                        outputs.push(match &outputs[*first] {
                            Output::Text(masked) => Output::Text(masked.clone()),
                            Output::Fallback => Output::Fallback,
                            Output::Refused(message) => Output::Refused(message.clone()),
                            Output::Null => Output::Null,
                            Output::Int(number) => Output::Int(number.clone()),
                        });
                        continue;
                    }
                    seen.insert(text.as_str(), index);
                }
                outputs.push(self.strategy.maskOne(&self.hash, input));
            }

            outputs
        });

        // Build results with it again.
        let masked = PyList::empty(py);
        let problems = PyDict::new(py);
        for (index, output) in outputs.into_iter().enumerate() {
            match output {
                Output::Null => masked.append(py.None())?,
                Output::Int(number) => masked.append(number)?,
                Output::Text(text) => masked.append(text)?,
                Output::Fallback => {
                    masked.append(py.None())?;
                    problems.set_item(index, FALLBACK)?;
                }
                Output::Refused(message) => {
                    masked.append(py.None())?;
                    problems.set_item(index, (REFUSED, message))?;
                }
            }
        }

        Ok((masked, problems))
    }
}

#[pymodule]
fn bauta_rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add("FALLBACK", FALLBACK)?;
    module.add("REFUSED", REFUSED)?;
    module.add_class::<Masker>()?;

    Ok(())
}
