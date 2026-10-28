# SPDX-License-Identifier: MIT
"""Synthetic hydration tests; no real source text or network requests."""
import copy, email, gzip, hashlib, http.client, io, json, sqlite3, sys, tempfile, unittest
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hydration_common as common
import rehydrate_usenet as usenet
import refresh_usenet_sources as refresh

class Response(io.BytesIO):

    def __init__(self, raw=b'', headers=None, status=200):
        super().__init__(raw)
        self.headers = headers or {'Content-Length': str(len(raw))}
        self.status = status

class Opener:

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout=None):
        self.requests.append(request)
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

class TransportTests(unittest.TestCase):

    def net(self, tmp, opener, **kwargs):
        return common.Network(tmp, 'opensubtitles', opener=opener, sleeper=lambda x: None, **kwargs)

    def test_safe_hosts_never_receive_credentials_on_other_domains(self):
        self.assertFalse(common.official_url('https://api.opensubtitles.com.evil.test/api/v1/login', 'opensubtitles', True))
        self.assertFalse(common.official_url('http://api.opensubtitles.com/api/v1/login', 'opensubtitles', True))
        self.assertFalse(common.official_url('https://user:pass@archive.org/download/x', 'archive'))
        self.assertTrue(common.official_url('https://ia801500.us.archive.org/20/items/x/y', 'archive'))

    def test_auth_error_is_not_retried(self):
        with tempfile.TemporaryDirectory() as tmp:
            op = Opener(HTTPError('synthetic', 401, 'unauthorized', {}, None))
            with self.assertRaisesRegex(common.Outcome, 'authentication_'):
                self.net(tmp, op).json('https://api.opensubtitles.com/api/v1/login', body={'password': 'synthetic'}, api=True)
            self.assertEqual(len(op.requests), 1)

    def test_get_api_503_retries_and_counts_failed_requests(self):
        with tempfile.TemporaryDirectory() as tmp:
            op = Opener(HTTPError('synthetic', 503, 'failure', {}, None), Response(b'{"data":[]}'))
            n = self.net(tmp, op)
            self.assertEqual(n.json('https://api.opensubtitles.com/api/v1/subtitles', api=True), {'data': []})
            self.assertEqual(n.state['requests'], 2)

    def test_post_download_is_not_retried_after_uncertain_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            op = Opener(HTTPError('synthetic', 503, 'failure', {}, None))
            with self.assertRaisesRegex(common.Outcome, 'http_503'):
                self.net(tmp, op).json('https://api.opensubtitles.com/api/v1/download', body={'file_id': 777}, api=True)
            self.assertEqual(len(op.requests), 1)

    def test_rate_limit_with_long_retry_after_yields_resumable_outcome(self):
        with tempfile.TemporaryDirectory() as tmp:
            op = Opener(HTTPError('synthetic', 429, 'limit', {'Retry-After': '3600'}, None))
            n = self.net(tmp, op)
            with self.assertRaisesRegex(common.Outcome, 'resume_later'):
                n.json('https://api.opensubtitles.com/api/v1/subtitles', api=True)
            self.assertGreater(common.load(Path(tmp) / 'network-opensubtitles.json')['resume_after'], 0)

    def test_interrupted_download_resumes_with_range(self):

        class Broken(Response):

            def read(self, n=-1):
                if self.tell():
                    raise OSError('synthetic interrupted')
                return super().read(3)
        with tempfile.TemporaryDirectory() as tmp:
            op = Opener(Broken(b'abcdef', {'Content-Length': '6'}), Response(b'def', {'Content-Length': '3', 'Content-Range': 'bytes 3-5/6'}, 206))
            n = self.net(tmp, op)
            p = Path(tmp) / 'source.bin'
            n.download('https://dl.opensubtitles.com/synthetic', p, {'modern_file_id': 777}, 100)
            self.assertEqual(p.read_bytes(), b'abcdef')
            self.assertEqual(op.requests[1].get_header('Range'), 'bytes=3-')
            self.assertEqual(n.state['bytes'], 6)

    def test_ignored_range_restarts_instead_of_concatenating(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'source.bin'
            p.with_name('source.bin.part').write_bytes(b'abc')
            common.atomic_json(p.with_name('source.bin.part.json'), {'identity': {'modern_file_id': 777}, 'expected': {}})
            op = Opener(Response(b'abcdef'))
            n = self.net(tmp, op)
            n.download('https://dl.opensubtitles.com/synthetic', p, {'modern_file_id': 777}, 100)
            self.assertEqual(p.read_bytes(), b'abcdef')

    def test_cached_source_reused_but_tampering_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'source'
            n = self.net(tmp, Opener(Response(b'synthetic')))
            identity = {'modern_file_id': 777}
            n.download('https://dl.opensubtitles.com/synthetic', p, identity, 100)
            n.opener = Opener()
            n.download('https://dl.opensubtitles.com/synthetic', p, identity, 100)
            self.assertEqual(len(n.opener.requests), 0)
            p.write_bytes(b'changed')
            with self.assertRaisesRegex(common.Outcome, 'integrity_mismatch'):
                n.download('https://dl.opensubtitles.com/synthetic', p, identity, 100)

    def test_checksum_and_cumulative_bounds_are_enforced(self):
        with tempfile.TemporaryDirectory() as tmp:
            n = self.net(tmp, Opener(Response(b'synthetic')), max_requests=1)
            with self.assertRaises(common.Outcome):
                n.download('https://dl.opensubtitles.com/synthetic', Path(tmp) / 'source', {}, 100, {'sha1': '0' * 40})
            with self.assertRaisesRegex(common.Outcome, 'cumulative_'):
                n.json('https://api.opensubtitles.com/api/v1/subtitles', api=True)

    def test_unsafe_archive_entries_and_upload_output_rejected(self):
        for name in ['../outside', '/absolute', 'a/../../b', 'a\\b']:
            with self.assertRaises(common.Outcome):
                common.safe_member(name)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(common.Outcome):
                common.workspace(Path(tmp) / 'upload' / 'hydrated')

    def test_declared_oversize_asset_is_rejected_before_any_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            op = Opener()
            n = self.net(tmp, op)
            with self.assertRaisesRegex(common.Outcome, 'declared_file_exceeds'):
                n.download('https://dl.opensubtitles.com/synthetic', Path(tmp) / 'source', {}, 100, {'size': 101})
            self.assertEqual(op.requests, [])

    def test_api_response_bounds_and_interrupted_bytes_are_accounted(self):

        class BrokenAPI(Response):

            def read(self, n=-1):
                raise http.client.IncompleteRead(b'abc', 7)
        with tempfile.TemporaryDirectory() as tmp:
            op = Opener(BrokenAPI(b'', {'Content-Length': '10'}))
            n = self.net(tmp, op)
            with self.assertRaisesRegex(common.Outcome, 'interrupted_api_response'):
                n.json('https://api.opensubtitles.com/api/v1/subtitles', api=True)
            self.assertEqual(n.state['bytes'], 3)
        with tempfile.TemporaryDirectory() as tmp:
            n = self.net(tmp, Opener(Response(b'abcdefghij')), max_bytes=3)
            with self.assertRaisesRegex(common.Outcome, 'declared_byte_bound'):
                n.json('https://api.opensubtitles.com/api/v1/subtitles', api=True)
            self.assertEqual(n.state['bytes'], 0)

    def test_crash_after_receipt_before_rename_needs_no_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            p = root / 'source'
            part = root / 'source.part'
            part.write_bytes(b'synthetic')
            identity = {'modern_file_id': 777}
            common.atomic_json(root / 'source.part.json', {'identity': identity, 'expected': {}})
            common.atomic_json(root / 'source.receipt.json', {'identity': identity, 'sha256': common.sha_file(part), 'size': part.stat().st_size})
            op = Opener()
            n = self.net(tmp, op)
            n.download('https://dl.opensubtitles.com/synthetic', p, identity, 100)
            self.assertEqual(p.read_bytes(), b'synthetic')
            self.assertEqual(op.requests, [])

    def test_child_symlink_cannot_redirect_hydration_to_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            work = root / 'local'
            work.mkdir()
            outside = root / 'upload'
            outside.mkdir()
            (work / 'results').symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(common.Outcome, 'symlink_in_private'):
                common.workspace(work)

    def test_incremental_array_preserves_order_repeats_and_empty(self):
        self.assertEqual(list(common.array_records(io.BytesIO(b'[]\n'))), [])
        raw = json.dumps([{'a': 'Synthetic café'}] * 2, ensure_ascii=False).encode()
        self.assertEqual(len(list(common.array_records(io.BytesIO(raw)))), 2)
        for bad in [b'[{},]', b'[{', b'[] {}', b'[{"a":1,"a":2}]']:
            with self.assertRaises(common.Outcome):
                list(common.array_records(io.BytesIO(bad)))

class UsenetTests(unittest.TestCase):

    def test_exact_message_identity_rule_keeps_brackets_empty_and_absent_distinct(self):
        h = lambda value: hashlib.sha256(value.encode()).hexdigest()
        self.assertEqual(usenet.historical_message_code(' <Synthetic@Test>#1/1 '), h('<synthetic@test>'))
        self.assertNotEqual(usenet.historical_message_code('<a>'), usenet.historical_message_code('a'))
        self.assertIsNone(usenet.historical_message_code(None))
        self.assertIsNone(usenet.historical_message_code(' NoNe '))
        self.assertEqual(usenet.historical_message_code(''), h(''))

    def test_mbox_streaming_retains_duplicates_and_order(self):
        raw = b'From synthetic 1\nMessage-ID: <a>\n\nSynthetic one\n\nFrom synthetic 2\nMessage-ID: <a>\n\nSynthetic two\n'
        rows = list(usenet.mbox_messages(io.BytesIO(raw)))
        self.assertEqual([x[0] for x in rows], [0, 1])
        self.assertGreater(rows[1][1], rows[0][1])
        self.assertIn(b'Synthetic two', rows[1][2])

    def test_mbox_message_bytes_match_historical_mailbox_separator_semantics(self):
        import mailbox
        raw = b'From synthetic 1\nMessage-ID: <a>\n\nSynthetic first.\n\nFrom synthetic 2\nMessage-ID: <b>\n\nSynthetic second.\r\n\r\nFrom synthetic 3\nMessage-ID: <c>\n\nSynthetic final.\n\n'
        streamed = list(usenet.mbox_messages(io.BytesIO(raw)))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'synthetic.mbox'
            path.write_bytes(raw)
            old = mailbox.mbox(path, create=False)
            try:
                for ordinal, _, message in streamed:
                    self.assertEqual(message, old.get_bytes(ordinal, from_=False))
            finally:
                old.close()

    def test_oversized_message_is_not_silently_truncated(self):
        rows = list(usenet.mbox_messages(io.BytesIO(b'From synthetic\n\n' + b'x' * 100 + b'\n'), 10))
        self.assertIsNone(rows[0][2])

    def test_long_physical_line_and_expansion_bound_are_streamed(self):

        class BoundedReader(io.BytesIO):

            def readline(self, size=-1):
                if not 0 < size <= 65536:
                    raise AssertionError('unbounded physical line read')
                return super().readline(size)
        raw = b'From synthetic\nMessage-ID: <a>\n\n' + b'x' * 200000 + b'\n'
        result = list(usenet.mbox_messages(BoundedReader(raw), 512, len(raw)))
        self.assertIsNone(result[0][2])
        with self.assertRaisesRegex(common.Outcome, 'uncompressed_mbox_byte_bound'):
            list(usenet.mbox_messages(BoundedReader(raw), 512, 1024))

    def test_missing_turn_never_becomes_partial_dialogue(self):
        a = usenet.historical_message_code('<a>')
        b = usenet.historical_message_code('<b>')
        result = usenet.resolve_chain([a, b], {a: [[a, '2000/1/1', 'Synthetic', 'Synthetic body']]})
        self.assertFalse(result['complete'])
        self.assertIsNone(result['texts'])
        self.assertEqual(result['missing_codes'], [b])

    def test_first_row_behavior_duplicates_and_repeated_occurrences_preserved(self):
        a = usenet.historical_message_code('<a>')
        rows = [[a, '2000/1/1', 'Synthetic', 'First synthetic body'], [a, '2000/1/2', 'Synthetic', 'Other synthetic body']]
        result = usenet.resolve_chain([a, a], {a: rows})
        self.assertEqual(result['texts'], ['First synthetic body'] * 2)
        self.assertEqual(result['conflicting_codes'], [a, a])
        self.assertTrue(result['complete'])
        self.assertFalse(usenet.resolve_chain([a], {a: rows}, 'strict')['complete'])

    def test_header_match_alone_does_not_prove_body(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mbox = root / 'synthetic.mbox'
            mbox.write_bytes(b'From synthetic\nMessage-ID: <synthetic>\n\nSynthetic body\n')
            code = usenet.historical_message_code('<synthetic>')

            def reject(message):
                raise common.Outcome('unavailable', 'synthetic_cleaning_unavailable')
            db, counts = usenet.build_index(root / 'index.sqlite3', [{'path': str(mbox)}], {code}, 1000, 10000, processor=reject)
            self.assertEqual(counts['header_matches'], 1)
            self.assertEqual(db.execute('select count(*) from messages where cleaned is not null').fetchone()[0], 0)
            db.close()

    def test_index_resume_reuses_completed_cache_and_detects_changed_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            p = root / 'synthetic.mbox'
            p.write_bytes(b'From synthetic\nMessage-ID: <a>\n\nSynthetic body\n')
            code = usenet.historical_message_code('<a>')
            sources = [{'path': str(p)}]
            processor = lambda msg: [code, '2000/1/1', 'Synthetic', 'Synthetic body']
            db, c = usenet.build_index(root / 'index.sqlite3', sources, {code}, 1000, 10000, processor)
            db.close()
            db, c2 = usenet.build_index(root / 'index.sqlite3', sources, {code}, 1000, 10000, lambda msg: (_ for _ in ()).throw(AssertionError('must reuse')))
            db.close()
            self.assertEqual(c, c2)
            p.write_bytes(p.read_bytes() + b'Changed')
            with self.assertRaisesRegex(common.Outcome, 'inputs_or_recipe_changed'):
                usenet.build_index(root / 'index.sqlite3', sources, {code}, 1000, 10000, processor)

    def test_mime_uses_last_plain_part_but_top_level_charset_detection(self):
        message = EmailMessage()
        message.set_content('Synthetic first plain text long enough for selection and processing.')
        message.add_alternative('Synthetic last plain text long enough for selection and processing.', subtype='plain')
        calls = []

        def detector(body, **kwargs):
            return (True, len(body.encode()), [], [(0, len(body.encode()), 'SPANISH', 'es')])

        def charset(raw):
            calls.append(raw)
            return {'confidence': 0.99, 'encoding': 'utf-8'}
        body = usenet.historical_body(message, detector, charset)
        self.assertIn('Synthetic last', body)
        self.assertEqual(len(calls), 1)

    def test_declared_charset_is_strict_and_confidence_threshold_is_greater(self):
        message = email.message_from_bytes(b'Content-Type: text/plain; charset=ascii\n\nSynthetic \xff body long enough for testing decoding differences.')
        with self.assertRaisesRegex(common.Outcome, 'strict_decode_failed'):
            usenet.historical_body(message, lambda *a, **k: None, lambda b: None)
        message = email.message_from_bytes(b'Content-Type: text/plain\n\nSynthetic body long enough for testing decoding differences.')
        with self.assertRaisesRegex(common.Outcome, 'not_confident'):
            usenet.historical_body(message, lambda *a, **k: None, lambda b: {'confidence': 0.7, 'encoding': 'utf-8'})

    def test_historical_cld2_vector_length_as_end_defect_preserved(self):
        body = 'Synthetic prefix. ' + 'Synthetic Spanish body for documented vector selection.' * 3
        message = EmailMessage()
        message.set_content(body)

        def detector(text, **kwargs):
            return (True, len(text), [], [(18, 60, 'SPANISH', 'es')])
        selected = usenet.historical_body(message, detector, lambda b: None)
        self.assertEqual(selected, body[18:60].strip())
        self.assertNotEqual(selected, body[18:78].strip())

    def test_cleaning_preserves_documented_ineffective_wrote_condition(self):
        body = ' From: synthetic\n> Synthetic quoted\nSynthetic wrote:\nSynthetic retained\n\n'
        self.assertEqual(usenet.historical_clean_lines(body), 'Synthetic wrote: Synthetic retained')

    def test_exact_file_fingerprint_gate_and_incomplete_denominator(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = sqlite3.connect(':memory:')
            db.execute('CREATE TABLE messages (code TEXT,cleaned TEXT,source INTEGER,ordinal INTEGER)')
            code = usenet.historical_message_code('<a>')
            row = [code, '2000/1/1', 'Synthetic subject', 'Synthetic body']
            db.execute('INSERT INTO messages VALUES (?,?,0,0)', (code, common.canonical(row)))
            dialogue = {'dialog_hash': code + code, 'dialog_hash_chain': [code, code]}
            expected_json = {'rows': 1, 'original_records_sha256': common.sha_bytes(common.canonical({**dialogue, 'dialog_txt_chain': ['Synthetic body'] * 2}).encode())}
            expected_csv = {'rows': 1, 'original_records_sha256': common.sha_bytes((common.canonical(row) + '\n').encode())}
            result = usenet.reconstruct([dialogue], [row], db, expected_json, expected_csv, root / 'exact', 'historical-first')
            self.assertTrue(result['dialogue_file_exact_match'])
            self.assertTrue(result['message_file_semantic_exact_match'])
            expected_json['original_records_sha256'] = '0' * 64
            result = usenet.reconstruct([dialogue], [row], db, expected_json, expected_csv, root / 'mismatch', 'historical-first')
            self.assertFalse(result['dialogue_file_exact_match'])
            self.assertFalse((root / 'mismatch/verified-dialogue-file.json').exists())
            dialogue['dialog_hash_chain'].append('f' * 64)
            result = usenet.reconstruct([dialogue], [row], db, expected_json, expected_csv, root / 'missing', 'historical-first')
            self.assertEqual(result['incomplete_dialogues'], 1)
            self.assertEqual(result['reconstructed_complete_candidates'], 0)
            db.close()

    def test_metadata_refresh_is_exact_member_selection(self):
        d = {'containers': [{'container_key': 'synthetic', 'mapping_status': 'resolved', 'sources': [{'item_identifier': 'synthetic-item', 'member_filename': 'synthetic.mbox.zip', 'size': 12}]}]}
        result = refresh.resolve(d, {'synthetic-item': {'files': [{'name': 'other.mbox.zip', 'size': '12'}]}})
        self.assertEqual(result['containers'][0]['mapping_status'], 'unresolved')
        result = refresh.resolve(d, {'synthetic-item': {'files': [{'name': 'synthetic.mbox.zip', 'size': '13'}]}})
        self.assertTrue(result['containers'][0]['sources'][0]['recorded_snapshot_changed'])
        self.assertEqual(d['containers'][0]['sources'][0]['size'], 12)
if __name__ == '__main__':
    unittest.main()
