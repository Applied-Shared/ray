import sys
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.metrics import NoOpCounter, NoOpHistogram, NoOpUpDownCounter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from ray._private.metrics_agent import Gauge, Record
from ray._private.telemetry.open_telemetry_metric_recorder import (
    OpenTelemetryMetricRecorder,
)


def _gauge_values(recorder):
    """Returns the recorder's gauge observations with the per-entry TTL timestamp
    stripped, so tests can assert on the recorded values directly."""
    return {
        name: {tag_key: value for tag_key, (value, _ts) in observations.items()}
        for name, observations in recorder._gauge_observations_by_name.items()
    }


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_gauge_metric(mock_get_meter, mock_set_meter_provider):
    """
    Test the register_gauge_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers a gauge metric with the correct name and description.
    - Test that a value can be recorded for the gauge metric successfully.
    """
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_gauge_metric(name="test_gauge", description="Test Gauge")

    # Record a value for the gauge
    recorder.set_metric_value(
        name="test_gauge",
        tags={"label_key": "label_value"},
        value=42.0,
    )
    assert _gauge_values(recorder) == {
        "test_gauge": {
            frozenset({("label_key", "label_value")}): 42.0,
        }
    }


@patch("ray._private.telemetry.open_telemetry_metric_recorder.time.monotonic")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_gauge_value_retained_within_ttl_then_evicted(
    mock_get_meter, mock_set_meter_provider, mock_monotonic
):
    """
    A gauge value must survive scrapes for the TTL window (not be cleared after the
    first scrape), and be evicted once it has not been refreshed within the TTL.
    """
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder(gauge_metric_ttl_seconds=10.0)
    recorder.register_gauge_metric(name="g", description="g")
    callback = recorder._create_gauge_callback("g")

    # Report a value at t=1000.
    mock_monotonic.return_value = 1000.0
    recorder.set_metric_value(name="g", tags={"k": "v"}, value=7.0)

    # Scrape at t=1005 (within TTL): value is emitted.
    mock_monotonic.return_value = 1005.0
    assert [o.value for o in callback(None)] == [7.0]

    # Scrape again at t=1009 without re-reporting: still within TTL, so the value
    # persists (clear-on-scrape would have dropped it after the first scrape).
    mock_monotonic.return_value = 1009.0
    assert [o.value for o in callback(None)] == [7.0]

    # Scrape at t=1011 (>10s since the last report): the value is evicted.
    mock_monotonic.return_value = 1011.0
    assert callback(None) == []
    assert recorder._gauge_observations_by_name["g"] == {}


@patch("ray._private.telemetry.open_telemetry_metric_recorder.time.monotonic")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_gauge_refresh_extends_ttl(
    mock_get_meter, mock_set_meter_provider, mock_monotonic
):
    """Re-reporting a gauge value refreshes its TTL so it does not get evicted."""
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder(gauge_metric_ttl_seconds=10.0)
    recorder.register_gauge_metric(name="g", description="g")
    callback = recorder._create_gauge_callback("g")

    mock_monotonic.return_value = 1000.0
    recorder.set_metric_value(name="g", tags={"k": "v"}, value=7.0)

    # Re-report at t=1008 (within TTL): refreshes the timestamp.
    mock_monotonic.return_value = 1008.0
    recorder.set_metric_value(name="g", tags={"k": "v"}, value=7.0)

    # At t=1015 (>10s after the first report, but <10s after the refresh): still live.
    mock_monotonic.return_value = 1015.0
    assert [o.value for o in callback(None)] == [7.0]


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_counter_metric(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the register_counter_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers a counter metric with the correct name and description.
    - Test that a value can be set for the counter metric successfully without warnings.
    """
    mock_meter = MagicMock()
    mock_meter.create_counter.return_value = NoOpCounter(name="test_counter")
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_counter_metric(name="test_counter", description="Test Counter")
    assert "test_counter" in recorder._registered_instruments
    recorder.set_metric_value(
        name="test_counter",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    mock_logger_warning.assert_not_called()
    recorder.set_metric_value(
        name="test_counter_unregistered",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    mock_logger_warning.assert_called_once_with(
        "Unsupported synchronous instrument type for metric: test_counter_unregistered."
    )


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_sum_metric(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the register_sum_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers a sum metric with the correct name and description.
    - Test that a value can be set for the sum metric successfully without warnings.
    """
    mock_meter = MagicMock()
    mock_meter.create_up_down_counter.return_value = NoOpUpDownCounter(name="test_sum")
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_sum_metric(name="test_sum", description="Test Sum")
    assert "test_sum" in recorder._registered_instruments
    recorder.set_metric_value(
        name="test_sum",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    mock_logger_warning.assert_not_called()


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_histogram_metric(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the register_histogram_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers a histogram metric with the correct name and description.
    - Test that a value can be set for the histogram metric successfully without warnings.
    """
    mock_meter = MagicMock()
    mock_meter.create_histogram.return_value = NoOpHistogram(name="test_histogram")
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_histogram_metric(
        name="test_histogram", description="Test Histogram", buckets=[1.0, 2.0, 3.0]
    )
    assert "test_histogram" in recorder._registered_instruments
    recorder.set_metric_value(
        name="test_histogram",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    mock_logger_warning.assert_not_called()

    mock_meter.create_histogram.return_value = NoOpHistogram(name="neg_histogram")
    recorder.register_histogram_metric(
        name="neg_histogram",
        description="Histogram with negative first boundary",
        buckets=[-5.0, 0.0, 10.0],
    )

    mids = recorder.get_histogram_bucket_midpoints("neg_histogram")
    assert mids == pytest.approx([-7.5, -2.5, 5.0, 20.0])


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_record_and_export(mock_get_meter, mock_set_meter_provider):
    """
    Test the record_and_export method of OpenTelemetryMetricRecorder. Test that
    - The state of _observations_by_gauge_name is correct after recording a metric.
    - If there are multiple records with the same gauge name and tags, only the last
      value is kept.
    - If there are multiple records with the same gauge name but different tags, all
      values are kept.
    """
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder()
    recorder.record_and_export(
        [
            Record(
                gauge=Gauge(
                    name="hi",
                    description="Hi",
                    unit="unit",
                    tags={},
                ),
                value=1.0,
                tags={"label_key": "label_value"},
            ),
            Record(
                gauge=Gauge(
                    name="w00t",
                    description="w00t",
                    unit="unit",
                    tags={},
                ),
                value=2.0,
                tags={"label_key": "label_value"},
            ),
            Record(
                gauge=Gauge(
                    name="w00t",
                    description="w00t",
                    unit="unit",
                    tags={},
                ),
                value=20.0,
                tags={"another_label_key": "another_label_value"},
            ),
            Record(
                gauge=Gauge(
                    name="hi",
                    description="Hi",
                    unit="unit",
                    tags={},
                ),
                value=3.0,
                tags={"label_key": "label_value"},
            ),
        ],
        global_tags={"global_label_key": "global_label_value"},
    )
    assert _gauge_values(recorder) == {
        "hi": {
            frozenset(
                {
                    ("label_key", "label_value"),
                    ("global_label_key", "global_label_value"),
                }
            ): 3.0
        },
        "w00t": {
            frozenset(
                {
                    ("label_key", "label_value"),
                    ("global_label_key", "global_label_value"),
                }
            ): 2.0,
            frozenset(
                {
                    ("another_label_key", "another_label_value"),
                    ("global_label_key", "global_label_value"),
                }
            ): 20.0,
        },
    }


@pytest.mark.parametrize(
    "register_method,register_args",
    [
        ("register_gauge_metric", ("test_deadlock_gauge", "description")),
        ("register_counter_metric", ("test_deadlock_counter", "description")),
        ("register_sum_metric", ("test_deadlock_sum", "description")),
        # Histogram/counter/sum registration on 2.52.1 uses synchronous
        # instruments (not registered with the measurement consumer), so they
        # could not deadlock pre-fix; included as tripwires alongside gauge.
        (
            "register_histogram_metric",
            ("test_deadlock_histogram", "description", [1.0, 10.0]),
        ),
    ],
)
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_does_not_deadlock_with_concurrent_collect(
    mock_get_meter, mock_set_meter_provider, register_method, register_args
):
    """Regression test for an AB/BA deadlock between instrument registration and
    metric collection.

    The OpenTelemetry SDK holds an SDK-internal lock while invoking
    observable-instrument callbacks during collect(), and the recorder's
    callbacks acquire ``recorder._lock``. If registration holds
    ``recorder._lock`` across ``meter.create_*`` (which acquires that same
    SDK-internal lock), a registration that races a collect() deadlocks the
    process permanently:

        registration: recorder._lock -> SDK lock
        collect():    SDK lock       -> recorder._lock

    This test parks a real SDK collect() inside an instrument callback that
    contends on ``recorder._lock`` (exactly like the recorder's own callbacks
    do) while another thread registers a new instrument, and asserts that both
    complete.
    """
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder()
    # Use a local provider/reader so collect() exercises the real SDK locking,
    # independent of the process-global meter provider.
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    recorder.meter = provider.get_meter(__name__)

    in_callback = threading.Event()
    registration_started = threading.Event()

    def contending_callback(options):
        # Runs inside collect() with the SDK lock held.
        in_callback.set()
        registration_started.wait(timeout=30)
        # Contend on the recorder lock exactly like the recorder's own
        # observable callbacks do on every scrape.
        with recorder._lock:
            return []

    recorder.meter.create_observable_gauge(
        name="test_contending_gauge", callbacks=[contending_callback]
    )

    collect_thread = threading.Thread(target=reader.collect, daemon=True)
    collect_thread.start()
    assert in_callback.wait(timeout=10), "collect() never invoked the callback"

    register_thread = threading.Thread(
        target=getattr(recorder, register_method),
        args=register_args,
        daemon=True,
    )
    register_thread.start()
    # Let the registration reach its lock acquisitions before releasing the
    # callback. Under the pre-fix locking (recorder._lock held across
    # meter.create_*) recorder._lock is what becomes visibly held; under the
    # fixed locking it is _registration_lock — break on either so the barrier
    # is fast in both worlds instead of sleeping out the full budget.
    for _ in range(100):
        if (
            recorder._registration_lock.locked()
            or recorder._lock.locked()
            or not register_thread.is_alive()
        ):
            break
        time.sleep(0.01)
    registration_started.set()

    register_thread.join(timeout=10)
    registration_deadlocked = register_thread.is_alive()
    collect_thread.join(timeout=10)
    assert not registration_deadlocked, (
        f"{register_method}() deadlocked against a concurrent collect(); "
        "instrument creation must not run while holding recorder._lock."
    )
    assert not collect_thread.is_alive(), "collect() did not complete"
    with recorder._lock:
        assert register_args[0] in recorder._registered_instruments


if __name__ == "__main__":
    sys.exit(pytest.main(["-svv", __file__]))
