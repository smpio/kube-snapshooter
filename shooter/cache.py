import kubernetes.client
from kubernetes.config.dateutil import parse_rfc3339


class SnapshotCache:
    def __init__(self):
        self._ns_cache = {}
        self.api = kubernetes.client.CustomObjectsApi()

    def get_snapshots(self, namespace):
        try:
            return self._ns_cache[namespace]
        except KeyError:
            snaps = self._ns_cache[namespace] = self.api.list_namespaced_custom_object(
                group='snapshot.storage.k8s.io',
                version='v1',
                namespace=namespace,
                plural='volumesnapshots',
            )['items']
            for snap in snaps:
                snap['metadata']['creationTimestamp'] = parse_rfc3339(snap['metadata']['creationTimestamp'])
            return snaps
