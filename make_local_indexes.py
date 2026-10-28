#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Create a private exact-name index for user-supplied sources; no network.

Missing selected files remain in the index. Multiple supplied representations
require an explicit user choice and are never automatically preferred.
"""
import argparse
import collections
from pathlib import Path
import sys
from hydration_common import Outcome, atomic_json, canonical, cli_error, jsonl, load, sha_file, workspace


def build(a):
    if not 1 <= a.max_files <= 1000:
        raise Outcome('error','invalid_index_batch_bound')
    directory=a.source_dir.resolve();result={};counts=collections.Counter()
    if a.kind=='subtitles':
        selected=[]
        for row in jsonl(a.manifest):
            if a.language and row['published_language']!=a.language:continue
            if len(selected)<a.max_files:selected.append(row)
        for row in selected:
            paths=[directory/(row['native_file_id']+suffix)for suffix in ('.srt','.gz')]
            found=[p for p in paths if p.is_file()]
            if len(found)>1:raise Outcome('ambiguous','multiple_supplied_subtitle_files_require_explicit_index')
            path=found[0] if found else paths[0]
            result[row['file_key']]={'path':str(path),'input_adapter':'historical-gzip-latin1' if path.suffix=='.gz' else 'platform-utf8'}
            if found:result[row['file_key']]['sha256']=sha_file(path)
            counts['supplied' if found else 'unavailable']+=1
        protected=[a.manifest.parent]
    else:
        document=load(a.source_manifest)
        selected=[c for c in document['containers']if not a.container or c['container_key']in a.container][:a.max_files]
        if a.container and set(a.container)-{c['container_key']for c in selected}:
            raise Outcome('error','requested_container_missing_or_outside_batch')
        for c in selected:
            paths=[directory/(c['container_key']+suffix)for suffix in ('.mbox','.mbox.zip')]
            found=[p for p in paths if p.is_file()]
            if len(found)>1:raise Outcome('ambiguous','multiple_supplied_mboxes_require_explicit_index')
            result[c['container_key']]=[{'path':str(found[0])}]if found else []
            counts['supplied' if found else 'unavailable']+=1
        protected=[a.source_manifest.parent]
    workspace(a.output.parent,protected)
    atomic_json(a.output,result)
    return {'selected':len(result),'outcomes':dict(counts),'network_requests':0}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='kind',required=True)
    for kind in ('subtitles','usenet'):
        s=sub.add_parser(kind);s.add_argument('--source-dir',type=Path,required=True);s.add_argument('--output',type=Path,required=True);s.add_argument('--max-files',type=int,default=4)
        if kind=='subtitles':s.add_argument('--manifest',type=Path,required=True);s.add_argument('--language',choices=('es','ca','eu','gl'))
        else:s.add_argument('--source-manifest',type=Path,required=True);s.add_argument('--container',action='append')
    a=p.parse_args(argv)
    try:print(canonical(build(a)));return 0
    except (Outcome,OSError,ValueError,KeyError)as error:
        print(canonical(cli_error(error)),file=sys.stderr);return 2


if __name__=='__main__':raise SystemExit(main())
