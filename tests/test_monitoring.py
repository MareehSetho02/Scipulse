"""Contrats du monitoring : erreurs GX, données inconnues et fraîcheur réelle."""
from datetime import datetime, timedelta, timezone

import pytest

from src.pipeline.monitoring import build_snapshot, display_age, markdown, quality_statistics


def test_quality_rate_is_weighted_by_expectations_not_by_sources():
    report = {'statistics': [
        {'evaluated_expectations': 2, 'unsuccessful_expectations': 1},
        {'evaluated_expectations': 8, 'unsuccessful_expectations': 1}]}
    assert quality_statistics(report) == {'evaluated': 10, 'failed': 2, 'error_pct': 20.0}


@pytest.mark.parametrize('report', [{}, {'statistics': []}, {'statistics': [{'evaluated_expectations': 0}]}, {'statistics': [{'evaluated_expectations': 6}]}])
def test_missing_or_empty_quality_is_not_zero_percent(report):
    assert quality_statistics(report)['error_pct'] is None


def test_bronze_deposit_and_silver_publication_are_not_validation_timestamps():
    reference = datetime(2026, 10, 17, 12, tzinfo=timezone.utc)
    reports = {'silver_hn': {'timestamp': reference.isoformat(), 'quality_success': False,
                            'bronze_rows': 20, 'silver_rows': 18,
                            'statistics': [{'evaluated_expectations': 6, 'unsuccessful_expectations': 1}]}}
    snapshot = build_snapshot(reports, {'hn': reference - timedelta(minutes=5)},
                              {'hn': reference - timedelta(hours=2)},
                              {'rows': 100, 'updated_at': reference - timedelta(days=1)}, reference)
    hn = snapshot['sources']['hn']
    assert hn['bronze']['age_seconds'] == 300
    assert hn['silver']['age_seconds'] == 7200
    assert hn['last_quality_check']['age_seconds'] == 0
    assert snapshot['gold']['updated']['age_seconds'] == 86400
    rendered = markdown(snapshot)
    assert 'Échec — Silver non publiée' in rendered and '16.67 %' in rendered
    assert 'arxiv | Indisponible | Indisponible' in rendered
    assert 'pas le pourcentage de lignes incorrectes' in rendered


def test_unknown_gold_and_future_clock_are_visible():
    reference = datetime(2026, 10, 17, 12, tzinfo=timezone.utc)
    snapshot = build_snapshot({}, {'hn': reference + timedelta(minutes=1)}, {}, {}, reference)
    assert snapshot['gold']['rows'] is None
    assert snapshot['gold']['quality']['error_pct'] is None
    assert display_age(snapshot['sources']['hn']['bronze']).startswith('Horodatage futur')
    assert display_age(snapshot['sources']['arxiv']['bronze']) == 'Indisponible'


def test_monitoring_only_does_not_rewrite_insights(monkeypatch, tmp_path):
    from src.pipeline import dashboards
    calls = []
    snapshot = build_snapshot({}, {}, {}, {})
    monkeypatch.setattr(dashboards, 'api', lambda *a, **k: calls.append(('api', a)))
    monkeypatch.setattr(dashboards, 'metric', lambda *a, **k: calls.append(('metric', a)))
    monkeypatch.setattr(dashboards, 'save', lambda *a, **k: calls.append(('save', a)))
    monkeypatch.setattr(dashboards, 'dashboard', lambda *a, **k: calls.append(('dashboard', a)))
    monkeypatch.setattr(dashboards, 'save_report', lambda *a, **k: None)
    monkeypatch.setattr(dashboards, 'collect_snapshot', lambda: snapshot)
    dashboards.publish_monitoring()
    assert [a[0] for kind, a in calls if kind == 'dashboard'] == ['scipulse-monitoring']
    assert not any('scipulse-insights' in str(a) for _, a in calls)
