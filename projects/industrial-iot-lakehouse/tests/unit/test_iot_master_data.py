"""Master data and episodes (pure Python): deterministic, coherent and linked to failures."""

from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta
from itertools import pairwise

from iot_lakehouse.ingestion.source_contracts import IOT_SOURCE_CONTRACTS
from iot_lakehouse.synthetic import IotGenerationConfig, MasterData, generate_master_data
from iot_lakehouse.synthetic.physics import OPERATING_PROFILES, failure_modes_for


def test_same_seed_same_master_data(iot_config: IotGenerationConfig) -> None:
    assert generate_master_data(iot_config) == generate_master_data(iot_config)


def test_different_seed_different_failures(iot_config: IotGenerationConfig) -> None:
    other = generate_master_data(replace(iot_config, seed=iot_config.seed + 1))
    assert other.failures != generate_master_data(iot_config).failures


def test_equipment_and_sensor_counts(
    iot_master: MasterData, iot_config: IotGenerationConfig
) -> None:
    per_type = Counter(e.equipment_type for e in iot_master.equipment)
    assert set(per_type.values()) == {iot_config.equipment_per_type}
    for equipment in iot_master.equipment:
        sensors = {
            s.sensor_type for s in iot_master.sensors if s.equipment_id == equipment.equipment_id
        }
        assert sensors == set(OPERATING_PROFILES[equipment.equipment_type])


def test_failures_fall_in_the_window_and_never_overlap(
    iot_master: MasterData, iot_config: IotGenerationConfig
) -> None:
    window_end = iot_config.start + timedelta(days=iot_config.days)
    assert iot_master.failures, "the tiny configuration must produce failures"
    by_equipment: dict[str, list[tuple[datetime, datetime]]] = {}
    for failure in iot_master.failures:
        assert iot_config.start <= failure.failure_timestamp < window_end
        assert failure.degradation_start < failure.failure_timestamp < failure.downtime_end
        by_equipment.setdefault(failure.equipment_id, []).append(
            (failure.degradation_start, failure.downtime_end)
        )
    for windows in by_equipment.values():
        assert all(prev[1] <= nxt[0] for prev, nxt in pairwise(sorted(windows)))


def test_failure_modes_match_the_equipment_type(iot_master: MasterData) -> None:
    types = {e.equipment_id: e.equipment_type for e in iot_master.equipment}
    for failure in iot_master.failures:
        assert failure.failure_mode in {
            m.name for m in failure_modes_for(types[failure.equipment_id])
        }


def test_every_failure_has_one_corrective_order_closing_its_downtime(
    iot_master: MasterData,
) -> None:
    corrective = [o for o in iot_master.maintenance_orders if o.order_type == "corrective"]
    assert Counter(o.related_failure_id for o in corrective) == Counter(
        f.failure_id for f in iot_master.failures
    )
    failures = {f.failure_id: f for f in iot_master.failures}
    for order in corrective:
        failure = failures[str(order.related_failure_id)]
        assert failure.failure_timestamp <= order.opened_at <= order.started_at
        assert order.completed_at in (failure.downtime_end, None)


def test_preventive_orders_do_not_overlap_failure_windows(iot_master: MasterData) -> None:
    for order in (o for o in iot_master.maintenance_orders if o.order_type == "preventive"):
        assert order.related_failure_id is None
        for failure in (f for f in iot_master.failures if f.equipment_id == order.equipment_id):
            end = order.completed_at or order.started_at
            assert end <= failure.degradation_start or order.started_at >= failure.downtime_end


def test_degradation_episodes_follow_the_failure_mode(iot_master: MasterData) -> None:
    types = {e.equipment_id: e.equipment_type for e in iot_master.equipment}
    for failure in iot_master.failures:
        [mode] = [
            m
            for m in failure_modes_for(types[failure.equipment_id])
            if m.name == failure.failure_mode
        ]
        episodes = {
            (d.sensor_type, d.relative_change)
            for d in iot_master.degradation_episodes
            if d.failure_id == failure.failure_id
        }
        assert episodes == set(mode.effects)


def test_offline_periods_cover_failure_downtime_and_preventive_work(iot_master: MasterData) -> None:
    reasons = Counter(p.reason for p in iot_master.offline_periods)
    assert reasons["failure_downtime"] == len(iot_master.failures)
    assert reasons["preventive_maintenance"] == sum(
        1 for o in iot_master.maintenance_orders if o.order_type == "preventive"
    )
    assert reasons["communication_loss"] > 0
    assert all(p.start < p.end for p in iot_master.offline_periods)


def test_drift_episodes_are_sensor_faults_not_failures(iot_master: MasterData) -> None:
    assert iot_master.drift_episodes
    sensors = {s.sensor_id for s in iot_master.sensors}
    assert Counter(d.sensor_id for d in iot_master.drift_episodes).most_common(1)[0][1] == 1
    assert all(d.sensor_id in sensors and d.start < d.end for d in iot_master.drift_episodes)


def test_source_rows_follow_the_contracts(iot_master: MasterData) -> None:
    for dataset in ("equipment", "sensors", "failure_events", "maintenance_orders"):
        rows = iot_master.source_rows(dataset)
        assert rows
        assert all(list(row) == list(IOT_SOURCE_CONTRACTS[dataset].column_names) for row in rows)
