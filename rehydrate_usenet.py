#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Bounded Usenet recovery from public chains and local mbox / selected IA assets.

Default is a network-free plan. This independently written adapter preserves the
inspected historical algorithm, including its CLD2 vector slicing defect. It
does not import or relicense the older repository modules. Exact historical
execution versions are unknown; complete-file fingerprints are acceptance gates.
"""
from __future__ import annotations

import argparse
import collections
import csv
import email
from email.header import decode_header
from email import policy
from contextlib import contextmanager
import hashlib
import io
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import warnings
from urllib.parse import quote
import zipfile

from hydration_common import (MIB, Network, Outcome, add_network_arguments, array_records,
    atomic_json, canonical, cli_error, load, sha_file, sha_bytes, workspace, zip_infos)


def historical_message_code(header):
    # str(None) intentionally differs from str(''); angle brackets are retained.
    value = str(header).replace('#1/1', '').strip().lower()
    return None if value == 'none' else hashlib.sha256(value.encode('utf-8')).hexdigest()


def historical_body(message, detector=None, encoding_detector=None):
    """Historical last-plain-part, TOP-LEVEL charset and byte-vector slicing.

    CLD2 vector entries are (byte_offset, byte_length, language_name, code).
    The old code used body[offset:length] on a Unicode string. That defect is
    preserved here. Choosing a part's own charset or using offset+length would
    change the reproduction recipe and is deliberately not done.
    """
    if detector is None:
        try:
            import pycld2
            detector = pycld2.detect
        except ImportError:
            raise Outcome('error', 'install_requirements_hydration_for_historical_cleaning') from None
    if encoding_detector is None:
        try:
            import chardet
            encoding_detector = chardet.detect
        except ImportError:
            raise Outcome('error', 'install_requirements_hydration_for_historical_decoding') from None
    payload = None
    for part in message.walk():
        if not part.is_multipart() and part.get_content_type() == 'text/plain':
            payload = part.get_payload(decode=True)
    if payload is None:
        raise Outcome('unavailable', 'no_historical_plain_text_payload')
    charsets = message.get_charsets()
    charset = charsets[0] if charsets else None
    if charset is None or 'None' in charset:
        detected = encoding_detector(payload)
        if not detected.get('confidence', 0) > 0.7 or not detected.get('encoding'):
            raise Outcome('unavailable', 'historical_encoding_detection_not_confident')
        charset = detected['encoding'].lower()
    try:
        body = payload.decode(charset, errors='strict')
    except (UnicodeError, LookupError, AttributeError):
        raise Outcome('error', 'historical_strict_decode_failed') from None
    # The inspected implementation calls the detector twice. Preserve failures
    # in either invocation and its exact vector ordering, without a fallback LID.
    detector(body, returnVectors=True)
    reliable, _, _, vectors = detector(body, returnVectors=True)
    if not reliable:
        raise Outcome('unavailable', 'historical_language_detection_unreliable')
    selected = ''.join(body[v[0]:v[1]] + ' ' for v in vectors if v[3] == 'es').strip()
    if len(selected) < 30:
        raise Outcome('unavailable', 'historical_span_below_30_characters')
    return selected


def historical_clean_lines(body):
    kept = []
    for line in body.split('\n'):
        line = line.strip()
        if 'wrote:\n' in line.lower():  # ineffective historical condition retained
            continue
        if line.lower().startswith(('from:', 'to:', 'subject:', 'de:')) or line.startswith('>') or not line:
            continue
        kept.append(line)
    return ' '.join(kept).strip().replace('\n', ' ').replace('\r', ' ')


def historical_subject(header):
    parts = decode_header(header)
    # Preserve the old treatment of encoded chunks, including errors for bytes
    # lacking an encoding. Do not silently repair malformed subject headers.
    result = ''
    for value, encoding in parts:
        result += value.decode(encoding) if encoding else value
    return result.replace('\n', ' ').replace('\r', ' ')


def historical_row(message, detector=None, encoding_detector=None, date_parser=None):
    code = historical_message_code(message.get('Message-ID'))
    if code is None:
        raise Outcome('unavailable', 'historical_header_has_no_code')
    try:
        subject = historical_subject(message.get('Subject'))
        body = historical_clean_lines(historical_body(message, detector, encoding_detector))
        if date_parser is None:
            from dateutil.parser import parse as date_parser
        # Dateutil can embed a native timezone header fragment in a warning.
        # Preserve its calendar-date calculation without sending that fragment
        # to stderr or a public command log.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            date = date_parser(str(message.get('Date')), fuzzy=True)
    except Outcome:
        raise
    except ImportError:
        raise Outcome('error', 'install_requirements_hydration_for_date_parsing') from None
    except Exception:
        raise Outcome('error', 'historical_message_pipeline_failed') from None
    return [code, f'{date.year}/{date.month}/{date.day}', subject, body]


def mbox_messages(stream, maximum_message_bytes=16*MIB):
    """One message at a time, using the mailbox module's From-line boundaries.

    Escaped >From body lines stay escaped, matching mailbox.mbox retrieval.
    Oversized messages retain a position and explicit outcome, never truncate.
    """
    current = bytearray(); offset = 0; start = 0; number = 0; oversized = False; begun = False
    last_line_empty = False
    for line in stream:
        if line.startswith(b'From '):
            if begun:
                # CPython mailbox.mbox._generate_toc excludes one separator LF
                # exactly when its final physical line was b'\n'. This affects
                # CLD2 vector lengths; preserving an extra blank line changes
                # the historical Unicode slicing output for some messages.
                if last_line_empty and not oversized:
                    del current[-1:]
                yield number, start, None if oversized else bytes(current)
                number += 1
            begun = True; start = offset; current = bytearray(); oversized = False; last_line_empty = False
        elif not begun and line.strip():
            raise Outcome('error', 'mbox_missing_initial_unix_from_line')
        elif begun:
            last_line_empty = line == b'\n'
            if not oversized:
                current.extend(line)
                if len(current) > maximum_message_bytes:
                    current.clear(); oversized = True
        offset += len(line)
    if begun:
        if last_line_empty and not oversized:
            del current[-1:]
        yield number, start, None if oversized else bytes(current)


@contextmanager
def source_stream(path, expected_member=None):
    path = Path(path)
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            infos = [i for i in zip_infos(archive) if not i.is_dir()]
            candidates = [i for i in infos if i.filename == expected_member] if expected_member else [i for i in infos if i.filename.endswith('.mbox')]
            if len(candidates) != 1:
                raise Outcome('ambiguous', 'local_archive_mbox_member_not_unique')
            with archive.open(candidates[0]) as stream:
                yield stream
    else:
        if expected_member:
            raise Outcome('error', 'member_specified_for_plain_mbox')
        with path.open('rb') as stream:
            yield stream


@contextmanager
def nested_archive(outer_path, member):
    # Stream a dehydrated nested ZIP to a disk-backed temporary file. The
    # hydrated corpus is neither copied nor loaded into memory.
    with zipfile.ZipFile(outer_path) as outer:
        zip_infos(outer)
        with tempfile.TemporaryFile() as spool:
            with outer.open(member) as stream:
                for block in iter(lambda: stream.read(MIB), b''):
                    spool.write(block)
            spool.seek(0)
            with zipfile.ZipFile(spool) as inner:
                zip_infos(inner)
                yield inner


def released_container(archive, verification, container):
    json_stage = next(s for s in verification['stages'] if s['kind'] == 'json')
    csv_stage = next(s for s in verification['stages'] if s['kind'] == 'csv')
    expected_json = next((f for f in json_stage['files'] if f['member'] == container['dialogue_member']), None)
    expected_csv = next((f for f in csv_stage['files'] if f['member'] == container['message_member']), None)
    if expected_json is None or expected_csv is None:
        raise Outcome('error', 'source_manifest_not_in_public_verification')
    with nested_archive(archive, json_stage['outer_member']) as inner:
        with inner.open(container['dialogue_member']) as stream:
            dialogues = list(array_records(stream))
    digest = hashlib.sha256(); turns = 0
    for item in dialogues:
        if set(item) != {'dialog_hash', 'dialog_hash_chain'} or not isinstance(item['dialog_hash_chain'], list):
            raise Outcome('error', 'unexpected_dehydrated_dialogue_fields')
        if any(not isinstance(c, str) or not re.fullmatch('[0-9a-f]{64}', c) for c in item['dialog_hash_chain']):
            raise Outcome('error', 'invalid_message_code_in_chain')
        if item['dialog_hash'] != ''.join(item['dialog_hash_chain']):
            raise Outcome('error', 'dialogue_identifier_differs_from_chain')
        digest.update(canonical(item).encode('utf-8')); turns += len(item['dialog_hash_chain'])
    if digest.hexdigest() != expected_json['dehydrated_records_sha256'] or len(dialogues) != expected_json['rows'] or turns != expected_json['turn_occurrences']:
        raise Outcome('mismatch', 'dehydrated_dialogue_fingerprint_or_count_differs')
    with nested_archive(archive, csv_stage['outer_member']) as inner:
        with inner.open(container['message_member']) as raw:
            with io.TextIOWrapper(raw, encoding='utf-8', errors='strict', newline='') as text:
                rows = list(csv.reader(text, delimiter=';', strict=True))
    digest = hashlib.sha256()
    for row in rows:
        if len(row) != 4 or row[2:] != ['[dehydrated]', '[dehydrated]']:
            raise Outcome('error', 'unexpected_dehydrated_csv_fields')
        digest.update((canonical(row) + '\n').encode('utf-8'))
    if digest.hexdigest() != expected_csv['dehydrated_records_sha256'] or len(rows) != expected_csv['rows']:
        raise Outcome('mismatch', 'dehydrated_message_fingerprint_or_count_differs')
    return dialogues, rows, expected_json, expected_csv


def build_index(path, sources, wanted, max_message_bytes, max_source_bytes, processor=historical_row):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fingerprints = [{'sha256': sha_file(s['path']), 'member': s.get('member')} for s in sources]
    target_hash = sha_bytes(canonical(sorted(wanted)).encode('utf-8'))
    receipt_path = path.with_suffix('.json')
    configuration = {'sources': fingerprints, 'wanted_sha256': target_hash,
                     'recipe': 'usenet-inspected-historical/1', 'max_message_bytes': max_message_bytes,
                     'max_source_bytes': max_source_bytes,
                     'mbox_reader': 'cpython-mbox-separator-lf/1'}
    if receipt_path.exists() and path.exists():
        previous = load(receipt_path)
        if previous['configuration'] != configuration:
            raise Outcome('error', 'resume_mbox_inputs_or_recipe_changed')
        return sqlite3.connect(path), previous['counts']
    database = sqlite3.connect(path)
    database.execute('CREATE TABLE IF NOT EXISTS messages (source INTEGER, ordinal INTEGER, offset INTEGER, code TEXT, raw BLOB, cleaned TEXT, outcome TEXT, reason TEXT, PRIMARY KEY(source, ordinal))')
    database.execute('CREATE INDEX IF NOT EXISTS message_code ON messages(code,source,ordinal)')
    database.execute('CREATE TABLE IF NOT EXISTS progress (source INTEGER PRIMARY KEY, next_ordinal INTEGER, configuration TEXT)')
    database.execute('CREATE TABLE IF NOT EXISTS counters (source INTEGER, name TEXT, value INTEGER, PRIMARY KEY(source,name))')
    for source_index, source in enumerate(sources):
        token = canonical(configuration)
        progress = database.execute('SELECT next_ordinal, configuration FROM progress WHERE source=?', (source_index,)).fetchone()
        if progress and progress[1] != token:
            database.close(); raise Outcome('error', 'interrupted_index_inputs_changed')
        next_ordinal = progress[0] if progress else 0
        counts = collections.Counter(dict(database.execute('SELECT name,value FROM counters WHERE source=?', (source_index,))))
        consumed = 0
        with source_stream(source['path'], source.get('member')) as stream:
            for ordinal, offset, raw in mbox_messages(stream, max_message_bytes):
                if offset > max_source_bytes:
                    database.close(); raise Outcome('error', 'uncompressed_mbox_byte_bound')
                if ordinal < next_ordinal:
                    continue
                if raw is not None:
                    consumed = offset + len(raw)
                    if consumed > max_source_bytes:
                        database.close(); raise Outcome('error', 'uncompressed_mbox_byte_bound')
                counts['messages_scanned'] += 1
                if raw is None:
                    counts['oversized_messages'] += 1
                else:
                    message = email.message_from_bytes(raw, policy=policy.compat32)
                    code = historical_message_code(message.get('Message-ID'))
                    if code in wanted:
                        counts['header_matches'] += 1
                        try:
                            row = processor(message)
                            status, reason = 'cleaned-candidate', 'historical_pipeline_applied_not_body_verified'
                            counts['cleaned_candidates'] += 1
                        except Exception as error:
                            outcome = cli_error(error); row = None
                            status, reason = outcome['outcome'], outcome['reason']
                            counts['cleaning_' + status] += 1
                        database.execute('INSERT OR REPLACE INTO messages VALUES (?,?,?,?,?,?,?,?)',
                            (source_index, ordinal, offset, code, raw, canonical(row) if row is not None else None, status, reason))
                database.execute('INSERT OR REPLACE INTO progress VALUES (?,?,?)', (source_index, ordinal + 1, token))
                for name, value in counts.items():
                    database.execute('INSERT OR REPLACE INTO counters VALUES (?,?,?)', (source_index, name, value))
                if ordinal % 100 == 0:
                    database.commit()
        database.commit()
    totals = collections.Counter()
    for name, value in database.execute('SELECT name,SUM(value) FROM counters GROUP BY name'):
        totals[name] = value
    atomic_json(receipt_path, {'configuration': configuration, 'counts': dict(totals)})
    return database, dict(totals)


def resolve_chain(chain, index, duplicate_policy='historical-first'):
    texts = []; missing = []; conflicts = []; duplicate_codes = []
    for code in chain:
        matches = index.get(code, [])
        if not matches:
            missing.append(code); continue
        if len(matches) > 1:
            duplicate_codes.append(code)
        variants = {canonical(row) for row in matches}
        if len(variants) > 1:
            conflicts.append(code)
            if duplicate_policy == 'strict':
                continue
        texts.append(matches[0][3])  # First successful historical CSV row; repetitions are not removed.
    complete = not missing and not (conflicts and duplicate_policy == 'strict')
    return {'complete': complete, 'texts': texts if complete else None,
            'missing_codes': missing, 'conflicting_codes': conflicts, 'duplicate_codes': duplicate_codes,
            'selection': duplicate_policy}


def reconstruct(dialogues, published_rows, database, expected_json, expected_csv, output, duplicate_policy):
    output = Path(output); output.mkdir(parents=True, exist_ok=True, mode=0o700)
    index = collections.defaultdict(list)
    raw_candidates = []
    for code, cleaned in database.execute('SELECT code,cleaned FROM messages WHERE cleaned IS NOT NULL ORDER BY source,ordinal'):
        row = __import__('json').loads(cleaned)
        index[code].append(row); raw_candidates.append(row)
    relation_counts = collections.Counter(); incomplete = 0
    candidate_path = output / 'candidate-dialogues.jsonl'
    original_digest = hashlib.sha256(); candidate_count = 0
    with candidate_path.open('w', encoding='utf-8', newline='\n') as stream:
        for position, item in enumerate(dialogues):
            recovered = resolve_chain(item['dialog_hash_chain'], index, duplicate_policy)
            relation_counts['dialogues_with_duplicate_codes'] += bool(recovered['duplicate_codes'])
            relation_counts['dialogues_with_conflicting_matches'] += bool(recovered['conflicting_codes'])
            report = {'published_array_position': position, 'chain_complete': recovered['complete'],
                      'missing_codes': recovered['missing_codes'], 'conflicting_codes': recovered['conflicting_codes'],
                      'duplicate_codes': recovered['duplicate_codes'], 'body_exact_match': False}
            if recovered['complete']:
                candidate = {**item, 'dialog_txt_chain': recovered['texts']}
                original_digest.update(canonical(candidate).encode('utf-8'))
                report['candidate'] = candidate; candidate_count += 1
            else:
                incomplete += 1  # No partial chain is emitted as a dialogue.
            stream.write(canonical(report) + '\n')
    exact = not incomplete and original_digest.hexdigest() == expected_json['original_records_sha256'] and candidate_count == expected_json['rows']
    row_digest = hashlib.sha256()
    csv_bytes_digest = hashlib.sha256()
    with (output / 'candidate-messages.csv').open('w', encoding='utf-8', newline='') as stream:
        writer = csv.writer(stream, delimiter=';', quoting=csv.QUOTE_ALL, lineterminator='\r\n')
        for row in raw_candidates:
            row_digest.update((canonical(row) + '\n').encode('utf-8'))
            writer.writerow(row)
    csv_bytes_digest = sha_file(output / 'candidate-messages.csv')
    csv_exact = len(raw_candidates) == expected_csv['rows'] and row_digest.hexdigest() == expected_csv['original_records_sha256']
    id_date_order = [[r[0], r[1]] for r in raw_candidates] == [[r[0], r[1]] for r in published_rows]
    if exact:
        atomic_json(output / 'verified-dialogue-file.json', {'dialogue_member_original_records_sha256': original_digest.hexdigest(),
            'verified_array_position_range': [0, len(dialogues)], 'range_end_exclusive': True,
            'candidate_file': candidate_path.name, 'candidate_file_sha256': sha_file(candidate_path)})
    summary = {'outcome': 'exact-match' if exact else ('ambiguous' if relation_counts['dialogues_with_conflicting_matches'] else 'mismatch' if not incomplete else 'unavailable'),
        'header_identity_matched_unique_codes': len(index), 'source_text_recovered': database.execute('SELECT COUNT(*) FROM messages').fetchone()[0],
        'historically_cleaned_candidate_rows': len(raw_candidates), 'cleaned_body_identity': 'unverified_except_complete_exact_files',
        'expected_dialogues': len(dialogues), 'reconstructed_complete_candidates': candidate_count,
        'incomplete_dialogues': incomplete, 'dialogue_file_exact_match': exact,
        'actual_original_records_sha256': original_digest.hexdigest() if not incomplete else None,
        'expected_original_records_sha256': expected_json['original_records_sha256'],
        'message_file_semantic_exact_match': csv_exact,
        'message_file_bytes_exact_match': csv_bytes_digest == expected_csv.get('original_bytes_sha256'),
        'message_id_date_sequence_matches': id_date_order,
        'expected_message_rows': expected_csv['rows'], 'actual_message_rows': len(raw_candidates),
        'message_records_sha256': row_digest.hexdigest(), 'duplicate_policy': duplicate_policy,
        **relation_counts,
        'limits': ['Header hashes do not verify bodies.', 'Historical runtime/settings remain incompletely documented.',
                   'First-row selection preserves the observed algorithm; changed-source matches remain candidates until exact file verification.']}
    atomic_json(output / 'reconstruction.json', summary)
    return summary


def archive_sources(container, work, net, max_file_bytes, maximum_downloads):
    sources = container.get('sources', [])
    if container.get('mapping_status') != 'resolved' or not sources:
        raise Outcome('unavailable', 'archive_source_mapping_unresolved')
    if len(sources) > maximum_downloads:
        raise Outcome('error', 'archive_member_download_count_bound')
    result = []
    for source in sources:
        if not re.fullmatch('[A-Za-z0-9_.-]+', source.get('item_identifier', '')):
            raise Outcome('error', 'invalid_archive_item_identifier')
        member = source['member_filename']
        from hydration_common import safe_member
        safe_member(member)
        if source.get('mapping_basis') != 'exact_selected_export_filename_and_archive_metadata':
            raise Outcome('unavailable', 'unsupported_source_selection_evidence')
        url = 'https://archive.org/download/' + quote(source['item_identifier'], safe='') + '/' + quote(member, safe='/')
        target = work / 'downloads' / (sha_bytes((source['item_identifier'] + '/' + member).encode()) + '.zip')
        expected = {k: source[k] for k in ('size', 'sha1', 'md5') if source.get(k) is not None}
        net.download(url, target, {'item_identifier': source['item_identifier'], 'member_filename': member}, max_file_bytes, expected)
        result.append({'path': str(target), 'member': source.get('mbox_member')})
    return result


def run(args):
    if not 1 <= args.max_containers <= 100 or not 1 <= args.max_downloads <= 100:
        raise Outcome('error', 'invalid_batch_bounds')
    manifest = load(args.source_manifest); verification = load(args.verification)
    requested = set(args.container or [])
    containers = [c for c in manifest['containers'] if not requested or c['container_key'] in requested]
    if requested - {c['container_key'] for c in containers}:
        raise Outcome('error', 'requested_container_missing')
    containers = containers[args.offset:args.offset + args.max_containers]
    if args.offset < 0 or not containers:
        raise Outcome('error', 'empty_or_invalid_container_selection')
    plan = {'source': args.source, 'network_requests': 0, 'selected_containers': len(containers),
            'container_keys': [c['container_key'] for c in containers],
            'download_bytes_without_cache': sum(s.get('size', 0) for c in containers for s in c.get('sources', [])),
            'maximum_downloads': args.max_downloads, 'maximum_file_bytes': args.max_file_bytes,
            'maximum_uncompressed_mbox_bytes_per_container': args.max_mbox_bytes,
            'mapping_outcomes': dict(collections.Counter(c['mapping_status'] for c in containers))}
    if not args.execute:
        return plan
    work = workspace(args.work_dir, [args.archive, args.verification.parent, args.source_manifest.parent])
    net = Network(work, 'archive', args.max_requests, args.max_total_bytes, args.retries) if args.source == 'archive' else None
    local_index = load(args.local_index) if args.local_index else {}
    results = []; downloads_left = args.max_downloads
    for container in containers:
        key = container['container_key']; output = work / 'containers' / sha_bytes(key.encode())
        result = {'container_key': key, 'outcome': 'unavailable'}
        try:
            dialogues, published_rows, expected_json, expected_csv = released_container(args.archive, verification, container)
            result.update(expected_dialogues=len(dialogues), expected_message_rows=len(published_rows),
                          expected_turn_occurrences=sum(len(d['dialog_hash_chain']) for d in dialogues))
            if args.source == 'local':
                supplied = local_index.get(key, [])
                if args.mbox:
                    if len(containers) != 1:
                        raise Outcome('error', 'direct_mbox_requires_one_selected_container')
                    supplied = [{'path': str(p.resolve())} for p in args.mbox]
                if not supplied:
                    raise Outcome('unavailable', 'local_mbox_not_supplied')
                sources = []
                for item in supplied:
                    item = dict(item); path = Path(item['path']).expanduser()
                    if not path.is_absolute():
                        path = args.local_index.parent / path
                    if not path.is_file():
                        raise Outcome('unavailable', 'local_mbox_missing')
                    item['path'] = str(path); sources.append(item)
            else:
                needed = len(container.get('sources', []))
                sources = archive_sources(container, work, net, args.max_file_bytes, downloads_left)
                downloads_left -= needed
            wanted = {r[0] for r in published_rows}
            wanted.update(c for d in dialogues for c in d['dialog_hash_chain'])
            database, counts = build_index(output / 'messages.sqlite3', sources, wanted, args.max_message_bytes, args.max_mbox_bytes)
            try:
                result.update(reconstruct(dialogues, published_rows, database, expected_json, expected_csv, output, args.duplicate_policy))
                result['header_match_rows'] = database.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
                result['header_matches_without_cleaned_body'] = database.execute('SELECT COUNT(*) FROM messages WHERE cleaned IS NULL').fetchone()[0]
            finally:
                database.close()
            result.update(index_counts=counts, downloaded=args.source == 'archive')
        except (Outcome, OSError, ValueError, KeyError, zipfile.BadZipFile, sqlite3.Error) as error:
            result.update(cli_error(error))
        atomic_json(output / 'result.json', result); results.append(result)
    summary = {'schema_version': 'usenet-hydration-summary/1', 'selected_containers': len(containers),
               'outcomes': dict(collections.Counter(r['outcome'] for r in results)),
               'dialogue_candidates': sum(r.get('reconstructed_complete_candidates', 0) for r in results),
               'expected_dialogues': sum(r.get('expected_dialogues', 0) for r in results),
               'exact_dialogue_files': sum(r.get('dialogue_file_exact_match', False) for r in results),
               'exact_message_files': sum(r.get('message_file_semantic_exact_match', False) for r in results),
               'network': net.state if net else {'requests': 0, 'bytes': 0}, 'results': results,
               'scope': 'Bounded selected exports only; source availability, header identity, body recovery and exact reconstruction are separate.'}
    atomic_json(work / 'summary.json', summary)
    # All result fields are aggregate/fixed outcome codes. Native headers, paths,
    # messages, body strings and per-dialogue chains are confined to private files.
    return summary


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--archive', type=Path, required=True, help='Unchanged usenet-dehydrated-v3.zip')
    p.add_argument('--verification', type=Path, required=True)
    p.add_argument('--source-manifest', type=Path, required=True)
    p.add_argument('--container', action='append', help='Exact container_key in source-selection.json; repeatable.')
    p.add_argument('--max-containers', type=int, default=1)
    p.add_argument('--offset', type=int, default=0)
    p.add_argument('--source', choices=('local', 'archive'), default='local')
    p.add_argument('--mbox', type=Path, action='append', help='User-supplied mbox or ZIP; requires one selected container.')
    p.add_argument('--local-index', type=Path, help='Private JSON: container_key -> ordered list of {path, optional member}.')
    p.add_argument('--work-dir', type=Path, default=Path('local/usenet'))
    p.add_argument('--max-downloads', type=int, default=1)
    p.add_argument('--max-mbox-bytes', type=int, default=512*MIB)
    p.add_argument('--max-message-bytes', type=int, default=16*MIB)
    p.add_argument('--duplicate-policy', choices=('historical-first', 'strict'), default='historical-first')
    p.add_argument('--execute', action='store_true')
    p.add_argument('--dry-run', action='store_true')
    add_network_arguments(p, default_bytes=128*MIB)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.execute and args.dry_run:
            raise Outcome('error', 'execute_and_dry_run_are_exclusive')
        if min(args.max_mbox_bytes, args.max_message_bytes, args.max_file_bytes) <= 0:
            raise Outcome('error', 'invalid_byte_bounds')
        print(canonical(run(args))); return 0
    except (Outcome, OSError, ValueError, KeyError, UnicodeError) as error:
        print(canonical(cli_error(error)), file=sys.stderr); return 2


if __name__ == '__main__':
    raise SystemExit(main())
