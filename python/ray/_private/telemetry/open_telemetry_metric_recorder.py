import logging
import threading
import time
from collections import defaultdict
from typing import Callable, List, Optional

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider

import ray
from ray._private.metrics_agent import Record
from ray._private.telemetry.metric_cardinality import MetricCardinality

logger = logging.getLogger(__name__)

NAMESPACE = "ray"


class OpenTelemetryMetricRecorder:
    """
    A class to record OpenTelemetry metrics. This is the main entry point for exporting
    all ray telemetries to Prometheus server.
    It uses OpenTelemetry's Prometheus exporter to export metrics.
    """

    _metrics_initialized = False
    _metrics_initialized_lock = threading.Lock()

    def __init__(self, gauge_metric_ttl_seconds: Optional[float] = None):
        # Lock-ordering contract:
        #   _registration_lock -> SDK meter locks (via meter.create_*)
        #   _registration_lock -> _lock
        #   SDK measurement-consumer lock -> _lock (the SDK holds its own lock
        #       while invoking our observable callbacks during collect(), and the
        #       callbacks acquire _lock)
        # _lock must therefore never be held across an SDK call that can acquire
        # the measurement-consumer lock — i.e. observable-instrument creation
        # (meter.create_observable_*, which registers the instrument with the
        # consumer). Holding _lock there deadlocks with a concurrent scrape:
        # registration holds _lock and waits on the consumer lock inside
        # create_*, while collect() holds the consumer lock and waits on _lock
        # inside a callback. _registration_lock exists so that instrument
        # registration stays serialized (no duplicate instruments on a race)
        # without _lock being held across those SDK entry points.
        #
        # Note: the synchronous counter/sum/histogram paths in set_metric_value
        # do call instrument.add() / instrument.record() while holding _lock.
        # That is safe from this deadlock: they only take per-reader-storage
        # locks, never the measurement-consumer lock, and reader-storage code
        # never calls back into this recorder, so no lock cycle can form. Keep
        # any new SDK call out of _lock unless it has the same property.
        self._lock = threading.Lock()
        self._registration_lock = threading.Lock()
        self._registered_instruments = {}
        # Gauge observations are stored as tag_key -> (value, last_update_monotonic).
        # Gauges are evicted once they have not been refreshed within
        # `_gauge_metric_ttl_s` (see the scrape callback below).
        self._gauge_observations_by_name = defaultdict(dict)
        self._histogram_bucket_midpoints = defaultdict(list)
        self._gauge_metric_ttl_s = self._resolve_gauge_ttl_seconds(
            gauge_metric_ttl_seconds
        )
        self._init_metrics()
        self.meter = metrics.get_meter(__name__)

    @staticmethod
    def _resolve_gauge_ttl_seconds(override: Optional[float]) -> float:
        """Returns how long (in seconds) a gauge observation is retained without a
        refresh before it is evicted on scrape.

        Emitters export their live gauge values to the agent every export interval.
        The effective export interval is ``max(metrics_report_interval_ms, 1000)`` --
        the emitter floors it at 1000ms via ``SetReportInterval`` in
        ``src/ray/stats/stats.h`` (``GetReportInterval()`` is what actually drives the
        OTLP export in ``InitOpenTelemetryExporter``). We apply the same floor here so
        the TTL is exactly 2x the true export cadence, even when the raw config value
        is below 1000ms. Retaining a value for 2 export intervals lets an
        actively-reported series survive a missed/late export, while a series that
        stops being reported (finished task, dead worker) ages out after ~2 intervals.

        NOTE: this mirrors the ``max(..., 1000)`` clamp and the export cadence in
        ``stats.h``. If either changes, update this derivation.

        Callers may pass an explicit ``override`` (used by tests and available for
        future injection from above).
        """
        if override is not None:
            return override
        # Mirror the emitter's SetReportInterval floor (stats.h): the export cadence is
        # max(metrics_report_interval_ms, 1000ms), never less than 1s.
        effective_report_interval_ms = max(
            ray._config.metrics_report_interval_ms(), 1000
        )
        return 2 * effective_report_interval_ms / 1000.0

    def _create_gauge_callback(
        self, name: str
    ) -> Callable[[dict], List[Observation]]:
        def callback(options):
            with self._lock:
                # Gauges report the last value. Instead of clearing after each
                # scrape (which drops a series between reports if the emitter
                # hasn't re-reported in time), retain each value for a TTL and
                # evict only observations that have gone stale.
                stored = self._gauge_observations_by_name.get(name, {})
                now = time.monotonic()
                retained = {}
                observations = {}
                for tag_set, (val, ts) in stored.items():
                    if now - ts <= self._gauge_metric_ttl_s:
                        retained[tag_set] = (val, ts)
                        observations[tag_set] = val
                self._gauge_observations_by_name[name] = retained

                # Drop high cardinality from tag_set and sum up the value for
                # same tag set after dropping
                aggregated_observations = defaultdict(float)
                high_cardinality_labels = (
                    MetricCardinality.get_high_cardinality_labels_to_drop(name)
                )
                for tag_set, val in observations.items():
                    # Convert frozenset back to dict
                    tags_dict = dict(tag_set)
                    # Filter out high cardinality labels
                    filtered_tags = {
                        k: v
                        for k, v in tags_dict.items()
                        if k not in high_cardinality_labels
                    }
                    # Create a key for aggregation
                    filtered_key = frozenset(filtered_tags.items())
                    # Sum up values for the same filtered tag set
                    aggregated_observations[filtered_key] += val

                return [
                    Observation(val, attributes=dict(tag_set))
                    for tag_set, val in aggregated_observations.items()
                ]

        return callback

    def _init_metrics(self):
        # Initialize the global metrics provider and meter. We only do this once on
        # the first initialization of the class, because re-setting the meter provider
        # can result in loss of metrics.
        with self._metrics_initialized_lock:
            if self._metrics_initialized:
                return
            prometheus_reader = PrometheusMetricReader()
            provider = MeterProvider(metric_readers=[prometheus_reader])
            metrics.set_meter_provider(provider)
            self._metrics_initialized = True

    def register_gauge_metric(self, name: str, description: str) -> None:
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Gauge with the same name is already registered.
                    return

            callback = self._create_gauge_callback(name)
            # Created without holding self._lock: create_observable_gauge acquires
            # SDK-internal locks that are also held around our callbacks during
            # collect() (see the lock-ordering contract in __init__).
            instrument = self.meter.create_observable_gauge(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            with self._lock:
                self._registered_instruments[name] = instrument
                self._gauge_observations_by_name[name] = {}

    def register_counter_metric(self, name: str, description: str) -> None:
        """
        Register a counter metric with the given name and description.
        """
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Counter with the same name is already registered. This is a
                    # common case when metrics are exported from multiple Ray
                    # components (e.g., raylet, worker, etc.) running in the same
                    # node. Since each component may export metrics with the same
                    # name, the same metric might be registered multiple times.
                    return

            # Created without holding self._lock (see __init__ lock-ordering
            # contract).
            instrument = self.meter.create_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
            )
            with self._lock:
                self._registered_instruments[name] = instrument

    def register_sum_metric(self, name: str, description: str) -> None:
        """
        Register a sum metric with the given name and description.
        """
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Sum with the same name is already registered. This is a common
                    # case when metrics are exported from multiple Ray components
                    # (e.g., raylet, worker, etc.) running in the same node. Since
                    # each component may export metrics with the same name, the same
                    # metric might be registered multiple times.
                    return

            # Created without holding self._lock (see __init__ lock-ordering
            # contract).
            instrument = self.meter.create_up_down_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
            )
            with self._lock:
                self._registered_instruments[name] = instrument

    def register_histogram_metric(
        self, name: str, description: str, buckets: List[float]
    ) -> None:
        """
        Register a histogram metric with the given name and description.
        """
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Histogram with the same name is already registered. This is a
                    # common case when metrics are exported from multiple Ray
                    # components (e.g., raylet, worker, etc.) running in the same
                    # node. Since each component may export metrics with the same
                    # name, the same metric might be registered multiple times.
                    return

            # Created without holding self._lock (see __init__ lock-ordering
            # contract).
            instrument = self.meter.create_histogram(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                explicit_bucket_boundaries_advisory=buckets,
            )

            # calculate the bucket midpoints; this is used for converting histogram
            # internal representation to approximated histogram data points.
            midpoints = []
            for i in range(len(buckets)):
                if i == 0:
                    lower_bound = 0.0 if buckets[0] > 0 else buckets[0] * 2.0
                    midpoints.append((lower_bound + buckets[0]) / 2.0)
                else:
                    midpoints.append((buckets[i] + buckets[i - 1]) / 2.0)
            # Approximated mid point for Inf+ bucket. Inf+ bucket is an implicit bucket
            # that is not part of buckets.
            midpoints.append(1.0 if buckets[-1] <= 0 else buckets[-1] * 2.0)

            with self._lock:
                self._registered_instruments[name] = instrument
                self._histogram_bucket_midpoints[name] = midpoints

    def get_histogram_bucket_midpoints(self, name: str) -> List[float]:
        """
        Get the bucket midpoints for a histogram metric with the given name.
        """
        return self._histogram_bucket_midpoints[name]

    def set_metric_value(self, name: str, tags: dict, value: float):
        """
        Set the value of a metric with the given name and tags. If the metric is not
        registered, it lazily records the value for observable metrics or is a no-op for
        synchronous metrics.
        """
        with self._lock:
            tag_key = frozenset(tags.items())
            if self._gauge_observations_by_name.get(name) is not None:
                # Gauge - store the most recent value and its timestamp for the given
                # tags. The timestamp is used to evict stale observations on scrape.
                self._gauge_observations_by_name[name][tag_key] = (
                    value,
                    time.monotonic(),
                )
            else:
                instrument = self._registered_instruments.get(name)
                tags = {
                    k: v
                    for k, v in tags.items()
                    if k
                    not in MetricCardinality.get_high_cardinality_labels_to_drop(name)
                }
                if isinstance(instrument, metrics.Counter):
                    instrument.add(value, attributes=tags)
                elif isinstance(instrument, metrics.UpDownCounter):
                    instrument.add(value, attributes=tags)
                elif isinstance(instrument, metrics.Histogram):
                    instrument.record(value, attributes=tags)
                else:
                    logger.warning(
                        f"Unsupported synchronous instrument type for metric: {name}."
                    )

    def record_and_export(self, records: List[Record], global_tags=None):
        """
        Record a list of telemetry records and export them to Prometheus.
        """
        global_tags = global_tags or {}

        for record in records:
            gauge = record.gauge
            value = record.value
            tags = {**record.tags, **global_tags}
            try:
                self.register_gauge_metric(gauge.name, gauge.description or "")
                self.set_metric_value(gauge.name, tags, value)
            except Exception as e:
                logger.error(
                    f"Failed to record metric {gauge.name} with value {value} with tags {tags!r} and global tags {global_tags!r} due to: {e!r}"
                )
