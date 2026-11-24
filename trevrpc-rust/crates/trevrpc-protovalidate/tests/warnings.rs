//! The warning registration logs for rules it cannot evaluate for an empty message. A test binary of
//! its own: `tracing` caches whether a callsite is enabled process-wide, so a test on another
//! thread that hits the warning with no subscriber would hide it from this one.

#![allow(clippy::unwrap_used)] // The test fails by panicking.

mod common;

use std::sync::{Arc, Mutex};

use common::validator;

/// Records the `message_type` field of every warning.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Warnings {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct MessageType(String);
        impl tracing::field::Visit for MessageType {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "message_type" {
                    value.clone_into(&mut self.0);
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        if *event.metadata().level() == tracing::Level::WARN {
            let mut message_type = MessageType(String::new());
            event.record(&mut message_type);
            self.0.lock().unwrap().push(message_type.0);
        }
    }
}

/// The warnings logged while `register` runs.
fn warnings(register: impl FnOnce()) -> Vec<String> {
    use tracing_subscriber::layer::SubscriberExt;
    let warnings = Warnings::default();
    let subscriber = tracing_subscriber::registry().with(warnings.clone());
    tracing::subscriber::with_default(subscriber, register);
    warnings.0.lock().unwrap().clone()
}

/// Registration warns about the rules it cannot evaluate for an empty message, which a rule naming a
/// missing field is among, and only about those.
#[test]
fn rules_that_cannot_be_evaluated_for_an_empty_message_are_logged() {
    let validator = validator();
    assert_eq!(
        warnings(|| {
            let _ = validator.message("test.rules.Request");
            let _ = validator.message("test.rules.MissingField");
            let _ = validator.message("test.rules.Divisor");
            // Each type warns once, however many methods take it and clients check it.
            let _ = validator.message("test.rules.MissingField");
            let _ = validator.clone().message("test.rules.Divisor");
        }),
        ["test.rules.MissingField", "test.rules.Divisor"]
    );
}
