def is_source_snapshot(snapshot, pvc_name):
    """Only snapshots created from this PVC participate in its retention."""
    source = snapshot.get('spec', {}).get('source', {})
    labels = snapshot.get('metadata', {}).get('labels', {})
    return (
        source.get('persistentVolumeClaimName') == pvc_name
        and bool(labels.get('snapshooter.smp.io/source'))
    )
