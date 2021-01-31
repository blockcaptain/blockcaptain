use std::fmt;

use libsystemd::logging::{journal_send, Priority};
use slog::{Drain, Key, OwnedKVList, Record, Serializer, KV};
pub struct JournalDrain;

impl Drain for JournalDrain {
    type Ok = ();
    type Err = libsystemd::errors::Error;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<(), Self::Err> {
        let mut serializer = JournalSerializer(Vec::<_>::default());

        let priority = match record.level() {
            slog::Level::Critical => Priority::Critical,
            slog::Level::Error => Priority::Error,
            slog::Level::Warning => Priority::Warning,
            slog::Level::Info => Priority::Notice,
            slog::Level::Debug => Priority::Info,
            slog::Level::Trace => Priority::Debug,
        };

        let _ = values.serialize(record, &mut serializer);
        let _ = record.kv().serialize(record, &mut serializer);

        journal_send(priority, &record.msg().to_string(), serializer.0.into_iter())
    }
}

struct JournalSerializer(pub Vec<(String, String)>);

impl Serializer for JournalSerializer {
    fn emit_arguments(&mut self, key: Key, value: &fmt::Arguments) -> slog::Result {
        self.0.push((Self::compliant_key(key), value.to_string()));
        Ok(())
    }
}

impl JournalSerializer {
    fn compliant_key(key: Key) -> String {
        // Until we find a non-underscore character, we can't output underscores for any other chars
        let mut found_non_underscore = false;
        let mut output = String::with_capacity(key.len());
        for c in key.chars() {
            match c {
                'A'..='Z' | '0'..='9' => {
                    output.push(c);
                    found_non_underscore = true;
                }
                'a'..='z' => {
                    output.push(c.to_ascii_uppercase());
                    found_non_underscore = true;
                }
                _ if found_non_underscore => output.push('_'),
                _ => {}
            }
        }
        output
    }
}
