use std::{
    fmt::Display,
    ops::{AddAssign, Deref},
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;

pub struct Latency(Histogram<u64>);

impl Deref for Latency {
    type Target = Histogram<u64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AddAssign<u64> for Latency {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs
    }
}

impl Default for Latency {
    fn default() -> Self {
        Self::new()
    }
}

impl Latency {
    pub fn new() -> Self {
        Self(Histogram::new(3).unwrap())
    }
}

pub struct Record<'a> {
    latency: &'a mut Latency,
    start: Instant,
}

impl Latency {
    pub fn record(&mut self) -> Record<'_> {
        Record {
            latency: self,
            start: Instant::now(),
        }
    }
}

impl Record<'_> {
    pub fn stop(self) {
        self.latency.0 += self.start.elapsed().as_nanos() as u64
    }
}

impl Display for Latency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mean = Duration::from_nanos(self.0.mean() as _);
        let p50 = Duration::from_nanos(self.0.value_at_percentile(50.) as _);
        let p90 = Duration::from_nanos(self.0.value_at_percentile(90.) as _);
        let p99 = Duration::from_nanos(self.0.value_at_percentile(99.) as _);
        let p999 = Duration::from_nanos(self.0.value_at_percentile(99.9) as _);
        let max = Duration::from_nanos(self.0.max() as _);
        write!(
            f,
            "mean {mean:?} p50 {p50:?} p90 {p90:?} p99 {p99:?} p99.9 {p999:?} max {max:?}"
        )
    }
}
