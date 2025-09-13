use std::{env, str::FromStr as _};

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    FmtSubscriber, filter::Targets, fmt::format::FmtSpan, layer::SubscriberExt,
    util::SubscriberInitExt as _,
};

pub fn init_logging() {
    fmt_common().finish().with(targets_layer()).init();
}

pub fn init_logging_file(log_file: std::fs::File) {
    fmt_common()
        .with_ansi(false)
        .with_writer(log_file)
        .finish()
        .with(targets_layer())
        .init();
}

fn fmt_common() -> tracing_subscriber::fmt::SubscriberBuilder {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        // .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_span_events(FmtSpan::CLOSE)
        .with_file(true)
        .with_line_number(true)
}

fn targets_layer() -> tracing_subscriber::filter::Targets {
    // https://docs.rs/tracing-subscriber/0.3.19/src/tracing_subscriber/fmt/mod.rs.html#1200
    match env::var("RUST_LOG") {
        Ok(var) => Targets::from_str(&var)
            .map_err(|e| {
                eprintln!("Ignoring `RUST_LOG={var:?}`: {e}");
            })
            .unwrap_or_default(),
        Err(env::VarError::NotPresent) => {
            Targets::new().with_default(FmtSubscriber::DEFAULT_MAX_LEVEL)
        }
        Err(e) => {
            eprintln!("Ignoring `RUST_LOG`: {e}");
            Targets::new().with_default(FmtSubscriber::DEFAULT_MAX_LEVEL)
        }
    }
}
