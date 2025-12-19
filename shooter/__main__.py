import logging
import datetime
import argparse

import kubernetes.client

from utils.kubernetes.config import configure
from utils.signal import install_shutdown_signal_handlers

from .cache import SnapshotCache


log = logging.getLogger(__name__)

schedule_annotation = 'snapshooter.smp.io/schedule'
snapclass_annotation = 'snapshooter.smp.io/snapclass'
maxcount_annotation = 'snapshooter.smp.io/maxcount'
source_label = 'snapshooter.smp.io/source'
default_schedule = 'daily'
default_maxcount = 7
schedule_intervals = {
    'daily': datetime.timedelta(days=1),
    'weekly': datetime.timedelta(days=7),
    'monthly': datetime.timedelta(days=30),
    'never': None,
}
allowed_advance = datetime.timedelta(hours=1)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--master', help='kubernetes api server url')
    arg_parser.add_argument('--in-cluster', action='store_true', help='configure with in-cluster config')
    arg_parser.add_argument('--log-level', default='WARNING')
    arg_parser.add_argument('--default-maxcount', default='', help='daily=7,weekly=2,monthly=1')
    args = arg_parser.parse_args()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.log_level)
    configure(args.master, args.in_cluster)
    install_shutdown_signal_handlers()

    default_maxcount_map = {
        'daily': 7,
        'weekly': 2,
        'monthly': 1,
        'never': 0,
    }
    default_maxcount_map.update(parse_assignment_list(args.default_maxcount))
    for k, v in default_maxcount_map.items():
        default_maxcount_map[k] = int(v)

    api = kubernetes.client.CustomObjectsApi()
    v1 = kubernetes.client.CoreV1Api()
    cache = SnapshotCache()

    for pvc in v1.list_persistent_volume_claim_for_all_namespaces().items:
        schedule = pvc.metadata.annotations.get(schedule_annotation, default_schedule).lower()
        if schedule not in schedule_intervals:
            schedule = default_schedule
        log.info('%s/%s: schedule %s', pvc.metadata.namespace, pvc.metadata.name, schedule)

        ns_snaps = cache.get_snapshots(pvc.metadata.namespace)
        pvc_snaps = filter(lambda s: s['spec']['source']['persistentVolumeClaimName'] == pvc.metadata.name and
                                     s['metadata']['labels'].get(source_label),
                           ns_snaps)
        pvc_snaps = sorted(pvc_snaps, key=lambda s: s['metadata']['creationTimestamp'])

        if schedule != 'never':
            if pvc_snaps:
                last_snap = pvc_snaps[-1]['metadata']['creationTimestamp']
                interval = schedule_intervals[schedule]
                if now() - last_snap >= interval - allowed_advance:
                    pvc_snaps.append(create_snapshot(pvc))
                else:
                    log.info('%s/%s: already exists %s', pvc.metadata.namespace, pvc.metadata.name, last_snap)
            else:
                pvc_snaps.append(create_snapshot(pvc))

        try:
            maxcount = int(pvc.metadata.annotations.get(maxcount_annotation, default_maxcount_map[schedule]))
        except ValueError:
            maxcount = 1
        delete_count = max(0, len(pvc_snaps) - maxcount)
        log.info('%s/%s: %d/%d snapshots exist, deleting %d',
                 pvc.metadata.namespace,
                 pvc.metadata.name,
                 len(pvc_snaps),
                 maxcount,
                 delete_count)
        snaps_to_delete = pvc_snaps[:delete_count]
        for snap in snaps_to_delete:
            api.delete_namespaced_custom_object(
                group='snapshot.storage.k8s.io',
                version='v1',
                namespace=snap['metadata']['namespace'],
                plural='volumesnapshots',
                name=snap['metadata']['name'],
            )


def create_snapshot(pvc):
    print(f'>>> {pvc.metadata.namespace}/{pvc.metadata.name}')

    api = kubernetes.client.CustomObjectsApi()
    ts = now().strftime('%Y-%m-%d-%H-%M-%S')

    manifest = {
        'apiVersion': 'snapshot.storage.k8s.io/v1',
        'kind': 'VolumeSnapshot',
        'metadata': {
            'name': f'{pvc.metadata.name}-{ts}',
            'labels': {
                source_label: pvc.metadata.name,
            },
        },
        'spec': {
            'source': {
                'persistentVolumeClaimName': pvc.metadata.name,
            },
        },
    }

    snapclass = pvc.metadata.annotations.get(snapclass_annotation)
    if snapclass:
        manifest['spec']['volumeSnapshotClassName'] = snapclass

    log.info('creating snapshot %s/%s', pvc.metadata.namespace, manifest['metadata']['name'])
    return api.create_namespaced_custom_object(
        group='snapshot.storage.k8s.io',
        version='v1',
        namespace=pvc.metadata.namespace,
        plural='volumesnapshots',
        body=manifest,
    )


def now():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)


def parse_assignment_list(instr):
    result = {}
    for item in instr.split(','):
        a, b = item.split('=', maxsplit=1)
        result[a.strip()] = b.strip()
    return result


if __name__ == '__main__':
    main()
