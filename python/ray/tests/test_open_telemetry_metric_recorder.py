import sys
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.metrics import NoOpCounter, NoOpHistogram, NoOpUpDownCounter

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-svv", __file__]))
