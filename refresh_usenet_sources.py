#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Refresh exact selected Internet Archive members; no broad newsgroup search.

Default is a network-free plan. --execute fetches item/collection metadata only;
no mbox or source-message bytes. Use a private --work-dir and --output. The
published source-selection file remains unchanged. Do not use this mode where
your local operating policy reserves source connections for a human operator.
"""
import argparse
import collections
from pathlib import Path
import sys
from urllib.parse import quote

from hydration_common import (Network, Outcome, atomic_json, canonical, cli_error,
                             load, sha_bytes, workspace)


def resolve(document, metadata):
    import copy
    result = copy.deepcopy(document)
    counts = collections.Counter()
    for container in result['containers']:
        resolved = []
        for source in container.get('sources', []):
            item = source['item_identifier']
            observed = metadata.get(item)
            if observed is None:
                counts['item_metadata_unavailable'] += 1
                continue
            members = [f for f in observed.get('files', []) if f.get('name') == source['member_filename']]
            if len(members) != 1:
                counts['missing_or_ambiguous_member'] += 1
                continue
            member = members[0]
            updated = dict(source)
            updated['metadata_sha256'] = sha_bytes(canonical(observed).encode('utf-8'))
            updated['metadata_verification'] = 'refreshed_official_metadata_api'
            updated['item_collections'] = observed.get('metadata', {}).get('collection', [])
            for key in ('size', 'sha1', 'md5'):
                if key in member:
                    updated[key] = int(member[key]) if key == 'size' else member[key]
            updated['recorded_snapshot_changed'] = any(source.get(k) is not None and source[k] != updated.get(k) for k in ('size', 'sha1', 'md5'))
            # A changed asset is kept visible, and requires explicit acceptance
            # of the new source-selection manifest before a subsequent retrieval.
            resolved.append(updated)
        container['sources'] = resolved
        container['mapping_status'] = 'resolved' if resolved else 'unresolved'
        counts[container['mapping_status']] += 1
    result['metadata_refresh_counts'] = dict(counts)
    result['metadata_api'] = 'https://archive.org/metadata/{item_identifier}'
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest', type=Path, required=True)
    p.add_argument('--work-dir', type=Path, default=Path('local/archive-metadata'))
    p.add_argument('--output', type=Path, default=Path('local/archive-metadata/source-selection.refreshed.json'))
    p.add_argument('--execute', action='store_true')
    p.add_argument('--metadata-dir', type=Path, help='Offline snapshots named ITEM.json; no network.')
    a = p.parse_args(argv)
    try:
        document = load(a.manifest)
        items = sorted({s['item_identifier'] for c in document['containers'] for s in c.get('sources', [])})
        if not a.execute and not a.metadata_dir:
            print(canonical({'outcome': 'planned', 'network_requests': 0, 'items': items,
                             'collection_context': ['giganews', 'usenethistorical'], 'maximum_metadata_requests': len(items) + 2}))
            return 0
        work = workspace(a.work_dir, [a.manifest.parent])
        snapshots = {}; failures = {}
        network = Network(work, 'archive', max_requests=20, max_bytes=16*1024*1024, retries=2) if not a.metadata_dir else None
        for item in items + ['giganews', 'usenethistorical']:
            try:
                if a.metadata_dir:
                    snapshots[item] = load(a.metadata_dir / (item + '.json'))
                else:
                    snapshots[item] = network.json('https://archive.org/metadata/' + quote(item, safe=''), limit=8*1024*1024)
                atomic_json(work / (item + '.json'), snapshots[item])
            except (Outcome, OSError, ValueError) as error:
                failures[item] = cli_error(error)
        output = a.output.resolve()
        if not output.is_relative_to(work):
            raise Outcome('error', 'metadata_output_must_be_inside_private_work_dir')
        result = resolve(document, snapshots)
        result['metadata_failures'] = failures
        result['collection_metadata_sha256'] = {k: sha_bytes(canonical(v).encode('utf-8')) for k, v in snapshots.items() if k in ('giganews', 'usenethistorical')}
        atomic_json(output, result)
        print(canonical({'outcomes': result['metadata_refresh_counts'], 'failed_items': sorted(failures),
                         'network_requests': network.state['requests'] if network else 0}))
        return 0
    except (Outcome, OSError, ValueError, KeyError) as error:
        print(canonical(cli_error(error)), file=sys.stderr); return 2


if __name__ == '__main__':
    raise SystemExit(main())
