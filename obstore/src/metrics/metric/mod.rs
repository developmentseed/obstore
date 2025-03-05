use parking_lot::Mutex;
use std::any::Any;
use std::borrow::Cow;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

mod counter;
mod duration;
mod gauge;
mod histogram;
#[allow(clippy::module_inception)]
mod metric;

pub(crate) use counter::*;
pub(crate) use duration::*;
pub(crate) use gauge::*;
pub(crate) use histogram::*;
pub(crate) use metric::*;

/// A `Registry` stores a map of metric names to `Instrument`
///
/// It allows retrieving them by name, registering new instruments and generating
/// reports of all registered instruments
#[derive(Debug, Default)]
pub struct Registry {
    /// A list of instruments indexed by metric name
    ///
    /// A BTreeMap is used to provide a consistent ordering
    instruments: Mutex<BTreeMap<&'static str, Box<dyn Instrument>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new `Metric` with the provided name and description
    ///
    /// ```
    /// use ::metric::{Registry, Metric, U64Counter};
    ///
    /// let registry = Registry::new();
    /// let counter: Metric<U64Counter> = registry.register_metric("metric_name", "description");
    /// ```
    ///
    /// Note: `&'static str` is intentionally used to ensure the metric name appears "in-the-plain"
    /// and can easily be searched for within the codebase
    ///
    pub fn register_metric<T>(&self, name: &'static str, description: &'static str) -> Metric<T>
    where
        T: MetricObserver,
        T::Options: Default,
    {
        self.register_metric_with_options(name, description, Default::default)
    }

    /// If a metric with the provided `name` already exists, returns it
    ///
    /// Otherwise, invokes `options` and creates a new Metric from the
    /// returned options, stores it in this `Registry` and returns it
    ///
    /// Panics if an `Instrument` has already been registered with this
    /// name but a different type
    ///
    /// ```
    /// use ::metric::{Registry, Metric, U64Histogram, U64HistogramOptions};
    ///
    /// let registry = Registry::new();
    /// let histogram: Metric<U64Histogram> = registry.register_metric_with_options(
    ///     "metric_name",
    ///     "description",
    ///     || U64HistogramOptions::new([10, 20, u64::MAX]),
    /// );
    /// ```
    ///
    pub fn register_metric_with_options<T: MetricObserver, F: FnOnce() -> T::Options>(
        &self,
        name: &'static str,
        description: &'static str,
        options: F,
    ) -> Metric<T> {
        self.register_instrument(name, move || Metric::new(name, description, options()))
    }

    /// If an instrument already exists with the provided `name`, returns it
    ///
    /// Otherwise, invokes `create` to create a new `Instrument`, stores it in this `Registry`,
    /// and returns it
    ///
    /// An application might choose to register a custom Instrument, instead of using a Metric,
    /// when it wishes to defer some computation to report time. For example, reporting
    /// metrics from systems that cannot be instrumented with `Metric` directly (e.g. jemalloc)
    ///
    /// Note: An instrument name is not required to match the metric name(s) reported by the
    /// instrument, however:
    ///
    /// - instruments will report in order of their instrument name
    /// - not all reporters may handle the same metric name being reported multiple times
    ///
    /// Panics if an `Instrument` has already been registered with this name but a different type
    ///
    /// Panics if the instrument name is illegal
    pub fn register_instrument<F: FnOnce() -> I, I: Instrument + Clone + 'static>(
        &self,
        name: &'static str,
        create: F,
    ) -> I {
        assert_legal_key(name);

        let mut instruments = self.instruments.lock();

        let instrument = match instruments.entry(name) {
            Entry::Occupied(o) => match o.get().as_any().downcast_ref::<I>() {
                Some(instrument) => instrument.clone(),
                None => panic!("instrument {name} registered with two different types"),
            },
            Entry::Vacant(v) => {
                let instrument = create();
                v.insert(Box::new(instrument.clone()));
                instrument
            }
        };

        instrument
    }

    /// Returns the already registered `Instrument` if any
    ///
    /// This is primarily useful for testing
    pub fn get_instrument<I: Instrument + Clone + 'static>(&self, name: &'static str) -> Option<I> {
        let instruments = self.instruments.lock();
        instruments
            .get(name)
            .map(|instrument| match instrument.as_any().downcast_ref::<I>() {
                Some(metric) => metric.clone(),
                None => panic!("instrument {name} registered with two different types"),
            })
    }

    /// Record the current state of every metric in this registry to the provided `Reporter`
    ///
    /// Will iterate through all registered metrics in alphabetical order and for each:
    /// - call start_metric once
    /// - call report_observation once for each set of attributes in alphabetical order
    /// - call finish_metric once complete
    pub fn report(&self, reporter: &mut dyn Reporter) {
        let instruments = self.instruments.lock();
        for instrument in instruments.values() {
            instrument.report(reporter)
        }
    }
}

/// `Instrument` is a type that knows how to write its observations to a `Reporter`
pub trait Instrument: std::fmt::Debug + Send + Sync {
    /// Record the current state of this metric to the provided `Reporter`
    ///
    /// Guaranteed to:
    /// - call start_metric once
    /// - call report_observation once for each set of attributes in alphabetical order
    /// - call finish_metric once complete
    fn report(&self, reporter: &mut dyn Reporter);

    /// Returns the type as [`Any`] so that it can be downcast to its underlying type
    fn as_any(&self) -> &dyn Any;
}

/// `Reporter` is the trait that should be implemented by anything that wants to
/// extract the state of all metrics within a `Registry` and export them
pub trait Reporter {
    /// Start recording the observations of a single metric
    ///
    /// Successive calls are guaranteed to not occur without an intervening
    /// call to finish_metric
    fn start_metric(
        &mut self,
        metric_name: &'static str,
        description: &'static str,
        kind: MetricKind,
    );

    /// Record an observation for the metric started by start_metric
    ///
    /// Must not be called without a prior call to start_metric with
    /// no intervening call to finish_metric
    fn report_observation(&mut self, attributes: &Attributes, observation: Observation);

    /// Finish recording a given metric
    ///
    /// Must not be called without a prior call to start_metric with
    /// no intervening call to finish_metric
    fn finish_metric(&mut self);
}

/// A set of observations for a particular metric
///
/// This is solely used by `RawReporter` to buffer up observations, the `Reporter`
/// trait streams `Observation` and does not perform intermediate aggregation
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ObservationSet {
    pub metric_name: &'static str,
    pub description: &'static str,
    pub kind: MetricKind,
    pub observations: Vec<(Attributes, Observation)>,
}

impl ObservationSet {
    /// Returns the observation for a given set of attributes if any
    pub fn observation(&self, attributes: impl Into<Attributes>) -> Option<&Observation> {
        let attributes = attributes.into();
        self.observations
            .iter()
            .find_map(|(a, o)| if a == &attributes { Some(o) } else { None })
    }
}

/// A `Reporter` that records the raw data submitted
#[derive(Debug, Clone, Default)]
pub struct RawReporter {
    completed: Vec<ObservationSet>,
    in_progress: Option<ObservationSet>,
}

impl Reporter for RawReporter {
    fn start_metric(
        &mut self,
        metric_name: &'static str,
        description: &'static str,
        kind: MetricKind,
    ) {
        assert!(self.in_progress.is_none(), "metric already in progress");
        self.in_progress = Some(ObservationSet {
            metric_name,
            description,
            kind,
            observations: Default::default(),
        })
    }

    fn report_observation(&mut self, attributes: &Attributes, observation: Observation) {
        let metric = self
            .in_progress
            .as_mut()
            .expect("metric should be in progress");
        metric.observations.push((attributes.clone(), observation))
    }

    fn finish_metric(&mut self) {
        let metric = self
            .in_progress
            .take()
            .expect("metric should be in progress");
        self.completed.push(metric)
    }
}

impl RawReporter {
    /// Returns the observation set for a given metric name if any
    pub fn metric(&self, metric_name: &str) -> Option<&ObservationSet> {
        self.observations()
            .iter()
            .find(|observation| observation.metric_name == metric_name)
    }

    /// Returns a list of `ObservationSet` for each reported metric
    pub fn observations(&self) -> &Vec<ObservationSet> {
        assert!(self.in_progress.is_none(), "metric observation in progress");
        &self.completed
    }
}

/// Identifies the type of `Observation` reported by this `Metric`
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MetricKind {
    U64Counter,
    U64Gauge,
    U64Histogram,
    DurationCounter,
    DurationGauge,
    DurationHistogram,
}

/// A `Metric` records an `Observation` for each unique set of `Attributes`
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Observation {
    U64Counter(u64),
    U64Gauge(u64),
    DurationCounter(std::time::Duration),
    DurationGauge(std::time::Duration),
    U64Histogram(HistogramObservation<u64>),
    DurationHistogram(HistogramObservation<std::time::Duration>),
}

/// A histogram measurement
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HistogramObservation<T> {
    /// The sum of all observations
    pub total: T,
    /// The buckets
    pub buckets: Vec<ObservationBucket<T>>,
}

impl<T> HistogramObservation<T> {
    pub fn sample_count(&self) -> u64 {
        self.buckets.iter().map(|bucket| bucket.count).sum()
    }
}

/// A bucketed observation
///
/// Stores the number of values that were less than or equal to `le` and
/// strictly greater than the `le` of the previous bucket
///
/// NB: Unlike prometheus histogram bins the buckets are not cumulative
/// i.e. `count` is just the count of values that fell into this bucket
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ObservationBucket<T> {
    pub le: T,
    pub count: u64,
}

/// A set of key-value pairs with unique keys
///
/// A `Metric` records observations for each unique set of `Attributes`
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Attributes(BTreeMap<&'static str, Cow<'static, str>>);

impl Attributes {
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, &'static str, Cow<'static, str>> {
        self.0.iter()
    }

    /// Sets the given key, overriding it if already set
    pub fn insert(&mut self, key: &'static str, value: impl Into<Cow<'static, str>>) {
        self.0.insert(key, value.into());
    }
}

impl<'a, const N: usize> From<&'a [(&'static str, &'static str); N]> for Attributes {
    fn from(iterator: &'a [(&'static str, &'static str); N]) -> Self {
        Self(
            iterator
                .iter()
                .map(|(key, value)| {
                    assert_legal_key(key);
                    (*key, Cow::Borrowed(*value))
                })
                .collect(),
        )
    }
}

impl<const N: usize> From<[(&'static str, Cow<'static, str>); N]> for Attributes {
    fn from(iterator: [(&'static str, Cow<'static, str>); N]) -> Self {
        Self(
            IntoIterator::into_iter(iterator)
                .map(|(key, value)| {
                    assert_legal_key(key);
                    (key, value)
                })
                .collect(),
        )
    }
}

/// Panics if the provided string matches [0-9a-z_]+
pub fn assert_legal_key(s: &str) {
    assert!(!s.is_empty(), "string must not be empty");
    assert!(
        s.chars().all(|c| matches!(c, '0'..='9' | 'a'..='z' | '_')),
        "string must be [0-9a-z_]+ got: \"{s}\""
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry() {
        let registry = Registry::new();
        let counter: Metric<U64Counter> = registry.register_metric("foo", "my magic description");
        let gauge: Metric<U64Gauge> = registry.register_metric("bar", "my magic description");

        counter.recorder(&[("tag1", "foo")]).inc(23);
        counter.recorder(&[("tag1", "bar")]).inc(53);
        gauge.recorder(&[("tag1", "value")]).set(49);

        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);

        let observations = reporter.observations();

        assert_eq!(observations.len(), 2);

        // Results should be alphabetical in metric name
        let gauge = &observations[0];
        assert_eq!(gauge.metric_name, "bar");
        assert_eq!(gauge.kind, MetricKind::U64Gauge);
        assert_eq!(gauge.observations.len(), 1);

        let (attributes, observation) = &gauge.observations[0];
        assert_eq!(attributes.0.get("tag1").unwrap(), "value");
        assert_eq!(observation, &Observation::U64Gauge(49));

        let counter = &observations[1];
        assert_eq!(counter.metric_name, "foo");
        assert_eq!(counter.kind, MetricKind::U64Counter);

        assert_eq!(counter.observations.len(), 2);

        // Attributes should be alphabetical
        let (attributes, observation) = &counter.observations[0];
        assert_eq!(attributes.0.get("tag1").unwrap(), "bar");
        assert_eq!(observation, &Observation::U64Counter(53));

        let (attributes, observation) = &counter.observations[1];
        assert_eq!(attributes.0.get("tag1").unwrap(), "foo");
        assert_eq!(observation, &Observation::U64Counter(23));

        assert!(registry
            .get_instrument::<Metric<U64Counter>>("unregistered")
            .is_none());

        let counter = registry
            .get_instrument::<Metric<U64Counter>>("foo")
            .unwrap();

        let new_attributes = Attributes::from(&[("foo", "bar")]);
        assert!(counter.get_observer(&new_attributes).is_none());
        let observation = counter.get_observer(attributes).unwrap().observe();

        assert_eq!(observation, Observation::U64Counter(23));
    }

    #[test]
    #[should_panic(expected = "instrument foo registered with two different types")]
    fn test_type_mismatch() {
        let registry = Registry::new();
        registry.register_metric::<U64Gauge>("foo", "my magic description");
        registry.register_metric::<U64Counter>("foo", "my magic description");
    }

    #[test]
    #[should_panic(expected = "string must be [0-9a-z_]+ got: \"foo sdf\"")]
    fn illegal_metric_name() {
        let registry = Registry::new();
        registry.register_metric::<U64Gauge>("foo sdf", "my magic description");
    }

    #[test]
    #[should_panic(expected = "string must be [0-9a-z_]+ got: \"foo bar\"")]
    fn illegal_attribute_name() {
        let _ = Attributes::from(&[("foo bar", "value")]);
    }
}
