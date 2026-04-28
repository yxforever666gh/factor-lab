from pathlib import Path

import pandas as pd

from factor_lab import workflow as workflow_module
from factor_lab.data import SampleDataset


class _FakeProvider:
    def route_healthy(self):
        return False


def test_load_dataset_uses_stale_cache_when_route_unhealthy(monkeypatch, tmp_path):
    cache_dir = tmp_path / 'artifacts' / 'tushare_cache'
    config = {
        'data_source': 'tushare',
        'cache_dir': str(cache_dir),
        'start_date': '2026-02-01',
        'end_date': '2026-03-18',
        'universe_limit': 30,
        'validation_mode': 'light_recent_window',
        'research_profile': 'opportunity_cheap_screen',
        'allow_stale_cache_days': 3,
    }

    monkeypatch.setenv('FACTOR_LAB_GENERATED_BATCH_DEFER_WHEN_ROUTE_UNHEALTHY', '1')
    monkeypatch.setattr(workflow_module, 'TushareDataProvider', _FakeProvider)
    monkeypatch.setattr(
        workflow_module,
        'inspect_feature_store_coverage',
        lambda universe_name, start_date, end_date, cache_dir='artifacts/tushare_cache': {
            'available': True,
            'covers_exact': False,
            'covers_start': True,
            'min_date': '2026-01-01',
            'max_date': '2026-03-16',
            'stale_days': 2,
            'effective_end_date': '2026-03-16',
        },
    )
    monkeypatch.setattr(workflow_module, 'ensure_feature_coverage', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('should not fetch when stale cache is acceptable')))
    monkeypatch.setattr(
        workflow_module,
        'slice_feature_store',
        lambda universe_name, start_date, end_date, cache_dir='artifacts/tushare_cache': SampleDataset(
            frame=pd.DataFrame(
                {
                    'date': pd.to_datetime(['2026-03-14', '2026-03-15']),
                    'ticker': ['000001.SZ', '000001.SZ'],
                    'close': [10.0, 10.5],
                }
            )
        ),
    )

    dataset = workflow_module._load_dataset(config)

    assert not dataset.frame.empty
    assert config['data_freshness'] == 'stale_acceptable_for_cheap_screen'
    assert config['effective_end_date'] == '2026-03-16'
