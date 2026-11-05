import unittest

from shooter.selection import is_source_snapshot


class SnapshotSelectionTests(unittest.TestCase):
    def test_import_does_not_break_source_retention(self):
        imported = {
            'metadata': {'labels': {'snapshooter.smp.io/source': 'db'}},
            'spec': {'source': {'volumeSnapshotContentName': 'import'}},
        }
        source = {
            'metadata': {'labels': {'snapshooter.smp.io/source': 'db'}},
            'spec': {'source': {'persistentVolumeClaimName': 'db'}},
        }
        selected = [
            s for s in [imported, source] if is_source_snapshot(s, 'db')
        ]
        self.assertEqual(selected, [source])

    def test_unlabelled_or_different_source_is_excluded(self):
        self.assertFalse(is_source_snapshot({
            'spec': {'source': {'persistentVolumeClaimName': 'db'}},
        }, 'db'))
        self.assertFalse(is_source_snapshot({
            'metadata': {'labels': {'snapshooter.smp.io/source': 'other'}},
            'spec': {'source': {'persistentVolumeClaimName': 'other'}},
        }, 'db'))


if __name__ == '__main__':
    unittest.main()
