use slog::{b, kv, o, Discard, Drain, Level, Logger, OwnedKVList, Record, KV};
use slog_term::{timestamp_local, CountingWriter, Decorator, RecordDecorator, Serializer};
use std::{fmt, io, io::Write, result};

fn print_msg_header(mut rd: &mut dyn RecordDecorator, record: &Record) -> io::Result<bool> {
    rd.start_timestamp()?;
    timestamp_local(&mut rd)?;

    rd.start_whitespace()?;
    write!(rd, " ")?;

    rd.start_level()?;
    write!(rd, "{}", record.level().as_short_str())?;

    rd.start_whitespace()?;
    write!(rd, " ")?;

    rd.start_msg()?;

    let count = {
        let mut count_rd = CountingWriter::new(&mut rd);
        write!(count_rd, "{}", record.msg())?;
        count_rd.count()
    };

    let module = record.location().module;
    if !module.starts_with("blkcapt") && !module.starts_with("libblkcapt") {
        rd.reset()?;
        // The tokio tracing module calls the wrong method on record builder and isnt giving
        // a static string for the module, thus <unknown>. I need to submit a PR to tokio
        // tracing. It does provide &'static str of module_path to "target" when target isn't
        // otherwise specified.
        let possible_module_path = match record.location().module {
            "<unknown>" => record.tag(),
            path => path,
        };
        write!(rd, " [{}]", possible_module_path)?;
    }

    Ok(count != 0)
}

pub struct CustomFullFormat<D>
where
    D: Decorator,
{
    decorator: D,
}

impl<D> Drain for CustomFullFormat<D>
where
    D: Decorator,
{
    type Ok = ();
    type Err = io::Error;

    fn log(&self, record: &Record, values: &OwnedKVList) -> result::Result<Self::Ok, Self::Err> {
        self.format_full(record, values)
    }
}

impl<D> CustomFullFormat<D>
where
    D: Decorator,
{
    pub fn new(decorator: D) -> Self {
        Self { decorator }
    }

    fn format_full(&self, record: &Record, values: &OwnedKVList) -> io::Result<()> {
        self.decorator.with_record(record, values, |decorator| {
            let comma_needed = print_msg_header(decorator, record)?;
            {
                let mut serializer = Serializer::new(decorator, comma_needed, false);

                record.kv().serialize(record, &mut serializer)?;

                values.serialize(record, &mut serializer)?;

                serializer.finish()?;
            }

            decorator.start_whitespace()?;
            writeln!(decorator)?;

            decorator.flush()?;

            Ok(())
        })
    }
}

// https://github.com/input-output-hk/jormungandr/pull/1509/files
/// slog serializers do not care about duplicates in KV fields that can occur
/// under certain circumstances. This serializer serves as a wrapper on top of
/// the actual serializer that takes care of duplicates. First it checks if the
/// same key was already. If so, this key is skipped during the serialization.
/// Otherwise the KV pair is passed to the inner serializer.
struct DedupSerializer<'a> {
    inner: &'a mut dyn slog::Serializer,
    seen_keys: std::collections::HashSet<slog::Key>,
}

impl<'a> DedupSerializer<'a> {
    fn new(inner: &'a mut dyn slog::Serializer) -> Self {
        Self {
            inner,
            seen_keys: Default::default(),
        }
    }
}

macro_rules! dedup_serializer_method_impl {
    ($(#[$m:meta])* $t:ty => $f:ident) => {
        $(#[$m])*
        fn $f(&mut self, key : slog::Key, val : $t)
            -> slog::Result {
                if self.seen_keys.contains(&key) {
                    return Ok(())
                }
                self.seen_keys.insert(key.clone());
                self.inner.$f(key, val)
            }
    };
}

impl<'a> slog::Serializer for DedupSerializer<'a> {
    dedup_serializer_method_impl! {
        &fmt::Arguments => emit_arguments
    }
    dedup_serializer_method_impl! {
        usize => emit_usize
    }
    dedup_serializer_method_impl! {
        isize => emit_isize
    }
    dedup_serializer_method_impl! {
        bool => emit_bool
    }
    dedup_serializer_method_impl! {
        char => emit_char
    }
    dedup_serializer_method_impl! {
        u8 => emit_u8
    }
    dedup_serializer_method_impl! {
        i8 => emit_i8
    }
    dedup_serializer_method_impl! {
        u16 => emit_u16
    }
    dedup_serializer_method_impl! {
        i16 => emit_i16
    }
    dedup_serializer_method_impl! {
        u32 => emit_u32
    }
    dedup_serializer_method_impl! {
        i32 => emit_i32
    }
    dedup_serializer_method_impl! {
        u64 => emit_u64
    }
    dedup_serializer_method_impl! {
        i64 => emit_i64
    }
    dedup_serializer_method_impl! {
        #[cfg(integer128)]
        u128 => emit_u128
    }
    dedup_serializer_method_impl! {
        #[cfg(integer128)]
        i128 => emit_i128
    }
    dedup_serializer_method_impl! {
        &str => emit_str
    }

    fn emit_unit(&mut self, key: slog::Key) -> slog::Result {
        if self.seen_keys.contains(&key) {
            return Ok(());
        }
        self.seen_keys.insert(key.clone());
        self.inner.emit_unit(key)
    }

    fn emit_none(&mut self, key: slog::Key) -> slog::Result {
        if self.seen_keys.contains(&key) {
            return Ok(());
        }
        self.seen_keys.insert(key.clone());
        self.inner.emit_none(key)
    }
}

/// The wrapper on top of an arbitrary KV object that utilize DedupSerializer.
struct DedupKV<T>(T);

impl<T: slog::KV> slog::KV for DedupKV<T> {
    fn serialize(&self, record: &slog::Record, serializer: &mut dyn slog::Serializer) -> slog::Result {
        let mut serializer = DedupSerializer::new(serializer);
        self.0.serialize(&record, &mut serializer)
    }
}

/// slog drain that uses DedupKV to remove duplicate keys from KV lists
pub struct DedupDrain<D> {
    inner: D,
}

impl<D> DedupDrain<D> {
    pub fn new(inner: D) -> Self {
        Self { inner }
    }
}

impl<D: Drain> Drain for DedupDrain<D> {
    type Ok = D::Ok;
    type Err = D::Err;

    fn log(&self, record: &slog::Record, values: &slog::OwnedKVList) -> Result<Self::Ok, Self::Err> {
        let values = slog::OwnedKV(DedupKV(values.clone()));
        self.inner.log(record, &values.into())
    }
}

pub struct SlogLogLogger(Logger);

fn log_to_slog_level(level: log::Level) -> Level {
    match level {
        log::Level::Trace => Level::Trace,
        log::Level::Debug => Level::Debug,
        log::Level::Info => Level::Info,
        log::Level::Warn => Level::Warning,
        log::Level::Error => Level::Error,
    }
}

fn record_as_location(r: &log::Record) -> slog::RecordLocation {
    let module = r.module_path_static().unwrap_or("<unknown>");
    let file = r.file_static().unwrap_or("<unknown>");
    let line = r.line().unwrap_or_default();

    slog::RecordLocation {
        file,
        line,
        column: 0,
        function: "",
        module,
    }
}

impl SlogLogLogger {
    pub fn install(log: Logger, level_filter: log::LevelFilter) {
        log::set_boxed_logger(Box::new(Self(log))).expect("no handling of set logger errors");
        log::set_max_level(level_filter);
    }
}

impl log::Log for SlogLogLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, r: &log::Record) {
        let level = log_to_slog_level(r.metadata().level());

        let args = r.args();
        let target = r.target();
        let location = &record_as_location(r);
        let s = slog::RecordStatic {
            location,
            level,
            tag: target,
        };
        self.0.log(&slog::Record::new(&s, args, b!()));
    }

    fn flush(&self) {}
}
