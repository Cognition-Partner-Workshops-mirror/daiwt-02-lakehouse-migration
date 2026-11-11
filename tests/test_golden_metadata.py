import json
from decimal import Decimal
from pathlib import Path

from migration_ci.goldens import column_type, input_entries, entries
from migration_ci.offline import compare


def test_normalized_golden_types():
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / 'golden/normalized-manifest.json').read_text())
    original = json.loads((root / 'golden/manifest.json').read_text())
    available = entries(manifest, 'D0')
    assert len(available) == 56
    assert len(input_entries(available)) == 38
    outputs = [entry for entry in available if entry['path'].startswith('outputs/')]
    assert len(outputs) == 18
    assert all(entry.get('table') for entry in outputs)
    for entry in available:
        assert entry['sha256'] == original['files'][entry['path']]['sha256']
        assert [column['name'] for column in entry['columns']] == original['files'][entry['path']]['columns']
        for column in entry['columns']:
            assert column['source_ddl']
            assert column['source_type']
            assert column_type(column)


def test_decimal_golden_uses_numeric_parity():
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / 'golden/normalized-manifest.json').read_text())
    sale = next(entry for entry in manifest['files'] if entry.get('table') == 'gold.fact_sale')
    price = next(column for column in sale['columns'] if column['name'] == 'unit_price')
    assert price['type'] == 'decimal(18,2)'
    assert compare([['230.00']], [(Decimal('230'),)], {'columns': [price]})['status'] == 'PASS'
