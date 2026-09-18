//! The PyO3 layer: conversions in, results out, and nothing else.
//!
//! Masking itself lives in `bauta-core`, which has no Python
//! dependency. Keeping the boundary thin is what lets the constructions be
//! tested without an interpreter.
//!
//! One call per column rather than per value. The interpreter is entered once
//! for the batch, and the work between the conversions runs with the GIL
//! released -- which is what lets a masking thread overlap with a reader and a
//! writer, and spread a chunk's values over several cores (`setThreads`).
//! Every mask depends on its value alone, so neither the thread count nor the
//! cache changes a result: only how soon it arrives.
//!
//! Values this crate does not handle are not errors. `Decimal`, `UUID`, dates,
//! bytes and non-ASCII text come back marked, and the Python layer masks those
//! itself, in position order, so a refusal Python would raise still wins.

#![allow(non_snake_case)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use rayon::prelude::*;

use num_bigint::BigInt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyString};

use bauta_core::cheap;
use bauta_core::{Charset, FakeKind, FakeLists, FakeStrategy, FpeStrategy, KeyStrategy, KeyedHash, MaskError};

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Values a `key`, `fpe` or `fake*` column remembers across chunks, and the
/// longest text it remembers. Foreign keys and statuses are mostly repeats, but seldom
/// within one chunk; the cache keeps the first values it sees rather than
/// churning, so a column of distinct values costs a lookup, not an eviction.
const CACHE_ENTRIES: usize = 65_536;
const CACHE_MAXIMUM_TEXT: usize = 64;

/// Distinct values in a call below which splitting it across threads costs
/// more than it saves.
const PARALLEL_MINIMUM: usize = 256;

/// The pool masking runs on in this process, or None for the calling thread
/// alone. Set by `setThreads`; one per process, shared by its columns.
static POOL: RwLock<Option<Arc<rayon::ThreadPool>>> = RwLock::new(None);

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

#[derive(Clone)]
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
    Fake(Box<FakeStrategy>),
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
            Strategy::Fake(_) => "fake",
        }
    }

    fn maskOne(&self, hash: &KeyedHash, input: &Input) -> Output {
        let result: Result<Output, MaskError> = match (self, input) {
            (_, Input::Null) => return Output::Null,
            (_, Input::Other) => return Output::Fallback,

            (Strategy::Key(_) | Strategy::Fpe(_), Input::Bool) => Err(MaskError::notABool(self.name())),
            // Keyed on the bytes Python keys them on: text as UTF-8, whatever
            // its script, and an integer's decimal digits.
            (Strategy::Fake(strategy), Input::Text(value)) => Ok(Output::Text(strategy.mask(hash, value.as_bytes()))),
            (Strategy::Fake(strategy), Input::Int(value)) => Ok(Output::Text(strategy.mask(hash, value.to_string().as_bytes()))),

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

/// A value as a cache or a batch's repeats know it.
#[derive(Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    Text(String),
    Int(BigInt),
}

impl CacheKey {
    fn of(input: &Input) -> Option<Self> {
        match input {
            Input::Text(text) => Some(CacheKey::Text(text.clone())),
            Input::Int(number) => Some(CacheKey::Int(number.clone())),
            _ => None,
        }
    }

    fn remembered(&self) -> bool {
        match self {
            CacheKey::Text(text) => text.len() <= CACHE_MAXIMUM_TEXT,
            CacheKey::Int(_) => true,
        }
    }
}

/// Where a position's answer comes from.
enum Source {
    Ready(Output),
    Computed(usize),
}

/// One column's masker: the key, the domain and the strategy, built once and
/// called per chunk.
#[pyclass]
struct Masker {
    hash: KeyedHash,
    strategy: Strategy,
    /// Masks remembered across calls, for the strategies that cost enough to
    /// be worth it; None for the rest.
    cache: Option<Mutex<HashMap<CacheKey, Output>>>,
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
            other => match FakeKind::parse(other) {
                Some(kind) => {
                    let list = |name: &str| -> PyResult<Vec<String>> {
                        options.get_item(name)?.ok_or_else(|| PyValueError::new_err(format!("{other} needs {name}")))?.extract()
                    };
                    let lists = FakeLists {
                        firstNames: list("firstNames")?,
                        lastNames: list("lastNames")?,
                        cities: list("cities")?,
                        streets: list("streets")?,
                        streetKinds: list("streetKinds")?,
                        address: text("address", "")?,
                        companySuffixes: list("companySuffixes")?,
                        companyWords: list("companyWords")?,
                    };
                    let maxLength = match options.get_item("maxLength")? {
                        Some(value) if !value.is_none() => Some(value.extract::<usize>()?),
                        _ => None,
                    };
                    Strategy::Fake(Box::new(FakeStrategy::new(kind, lists, maxLength).map_err(PyValueError::new_err)?))
                }
                None => return Err(PyValueError::new_err(format!("no native masker for strategy {other:?}"))),
            },
        };

        let cache = matches!(strategy, Strategy::Key(_) | Strategy::Fpe(_) | Strategy::Fake(_)).then(|| Mutex::new(HashMap::new()));

        Ok(Self { hash, strategy, cache })
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

        // Compute without it: each distinct value once, from the cache where it
        // can be, the rest across the pool.
        let outputs = py.detach(|| self.maskInputs(&inputs));

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

impl Masker {
    fn maskInputs(&self, inputs: &[Input]) -> Vec<Output> {
        let mut sources: Vec<Source> = Vec::with_capacity(inputs.len());
        let mut work: Vec<usize> = Vec::new();
        let mut firstOf: HashMap<CacheKey, usize> = HashMap::new();

        {
            let cache = self.cache.as_ref().map(|cache| cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));

            for (index, input) in inputs.iter().enumerate() {
                let Some(key) = CacheKey::of(input) else {
                    // Nulls and values Python masks are cheap to answer here.
                    sources.push(Source::Ready(self.strategy.maskOne(&self.hash, input)));
                    continue;
                };
                if let Some(output) = cache.as_ref().and_then(|cache| cache.get(&key)) {
                    sources.push(Source::Ready(output.clone()));
                    continue;
                }
                let slot = *firstOf.entry(key).or_insert_with(|| {
                    work.push(index);
                    work.len() - 1
                });
                sources.push(Source::Computed(slot));
            }
        }

        let mask = |index: &usize| self.strategy.maskOne(&self.hash, &inputs[*index]);
        let pool = POOL.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        let computed: Vec<Output> = match pool {
            Some(pool) if work.len() >= PARALLEL_MINIMUM => pool.install(|| work.par_iter().map(mask).collect()),
            _ => work.iter().map(mask).collect(),
        };

        if let Some(cache) = &self.cache {
            let mut cache = cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            for (slot, index) in work.iter().enumerate() {
                if cache.len() >= CACHE_ENTRIES {
                    break;
                }
                // Only answers: a refusal or a fallback is decided again, by
                // whichever implementation meets it first.
                if matches!(computed[slot], Output::Text(_) | Output::Int(_)) {
                    if let Some(key) = CacheKey::of(&inputs[*index]).filter(CacheKey::remembered) {
                        cache.insert(key, computed[slot].clone());
                    }
                }
            }
        }

        sources
            .into_iter()
            .map(|source| match source {
                Source::Ready(output) => output,
                Source::Computed(slot) => computed[slot].clone(),
            })
            .collect()
    }
}

/// The cores this process may use: a container's CPU quota on Linux, where
/// Python's os.cpu_count() reports the host's.
#[pyfunction]
fn availableCores() -> usize {
    std::thread::available_parallelism().map(|cores| cores.get()).unwrap_or(1)
}

/// How many threads mask a chunk in this process, 1 for the calling thread
/// alone. Results are the same for any count.
#[pyfunction]
fn setThreads(threads: usize) -> PyResult<()> {
    if threads == 0 {
        return Err(PyValueError::new_err("threads must be at least 1"));
    }
    let pool = if threads == 1 {
        None
    } else {
        let built = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("bauta-mask-{index}"))
            .build()
            .map_err(|error| PyValueError::new_err(format!("could not start {threads} masking threads: {error}")))?;
        Some(Arc::new(built))
    };
    *POOL.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = pool;

    Ok(())
}

/// The masking threads in this process.
#[pyfunction]
fn threads() -> usize {
    POOL.read().unwrap_or_else(|poisoned| poisoned.into_inner()).as_ref().map_or(1, |pool| pool.current_num_threads())
}

#[pymodule]
fn bauta_rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add("FALLBACK", FALLBACK)?;
    module.add("REFUSED", REFUSED)?;
    module.add_class::<Masker>()?;
    module.add_function(wrap_pyfunction!(availableCores, module)?)?;
    module.add_function(wrap_pyfunction!(setThreads, module)?)?;
    module.add_function(wrap_pyfunction!(threads, module)?)?;

    Ok(())
}
