# SPDX-License-Identifier: MIT
"""Small, bounded local hydration runtime. No network operation runs on import.

Only fixed error codes cross the CLI boundary. Provider payloads, URLs containing
tokens, credentials and source text are never printed. State belongs in the
user's private work directory, never in a deposit's upload directory.
"""
from __future__ import annotations

import hashlib
import http.client
import io
import json
import os
import re
import time
from email.utils import parsedate_to_datetime
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

MIB = 1024 * 1024
USER_AGENT = 'esCorpiusDialogRehydration v1.3'


class Outcome(Exception):
    def __init__(self, outcome, reason):
        self.outcome, self.reason = outcome, reason
        super().__init__(reason)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


def sha_bytes(value):
    return hashlib.sha256(value).hexdigest()


def sha_file(path, algorithm='sha256'):
    result = hashlib.new(algorithm)
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(MIB), b''):
            result.update(block)
    return result.hexdigest()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise Outcome('error', 'duplicate_json_key')
        result[key] = value
    return result


def parse(raw):
    return json.loads(raw, object_pairs_hook=_pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(Outcome('error', 'nonfinite_json')))


def load(path):
    return parse(Path(path).read_text(encoding='utf-8', errors='strict'))


def jsonl(path):
    with Path(path).open(encoding='utf-8', errors='strict', newline='') as stream:
        for line in stream:
            if not line.strip():
                raise Outcome('error', 'blank_manifest_row')
            yield parse(line)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise Outcome('error', 'symlink_output')
    temporary = path.with_name(path.name + '.next')
    if temporary.is_symlink():
        raise Outcome('error', 'symlink_checkpoint')
    # A crash may leave this tool's own uncommitted temporary checkpoint.
    with temporary.open('w', encoding='utf-8', newline='\n') as stream:
        os.chmod(temporary, 0o600)
        stream.write(canonical(value) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def workspace(path, protected=()):
    path = Path(path).expanduser().resolve()
    if 'upload' in path.parts or 'staging' in path.parts:
        raise Outcome('error', 'hydration_output_inside_release_staging')
    for item in protected:
        item = Path(item).resolve()
        root = item if item.is_dir() else item.parent
        if path == root or path.is_relative_to(root):
            raise Outcome('error', 'hydration_output_inside_public_inputs')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if any(p.is_symlink() for p in path.rglob('*')):
        raise Outcome('error', 'symlink_in_private_work_directory')
    return path


def safe_member(name):
    p = PurePosixPath(name)
    if not name or p.is_absolute() or '..' in p.parts or '\\' in name or '\x00' in name:
        raise Outcome('error', 'unsafe_archive_member')
    return name


def zip_infos(archive):
    result = archive.infolist()
    seen = set()
    for info in result:
        safe_member(info.filename)
        if info.filename in seen or ((info.external_attr >> 16) & 0o170000) == 0o120000:
            raise Outcome('error', 'duplicate_or_link_archive_entry')
        seen.add(info.filename)
    return result


def array_records(stream, maximum_record_bytes=32*MIB):
    """Incremental strict UTF-8 JSON-array reader, bounded by one record."""
    import codecs
    decoder = codecs.getincrementaldecoder('utf-8')('strict')
    parser = json.JSONDecoder(object_pairs_hook=_pairs)
    buffer = ''; position = 0; eof = False

    def more():
        nonlocal buffer, position, eof
        raw = stream.read(65536)
        buffer = buffer[position:] + decoder.decode(raw, final=not raw)
        position = 0
        eof = not raw

    def whitespace():
        nonlocal position
        while True:
            while position < len(buffer) and buffer[position].isspace():
                position += 1
            if position < len(buffer) or eof:
                return
            more()

    more(); whitespace()
    if position >= len(buffer) or buffer[position] != '[':
        raise Outcome('error', 'expected_json_array')
    position += 1; first = True
    while True:
        whitespace()
        if position < len(buffer) and buffer[position] == ']':
            position += 1; break
        if not first:
            if position >= len(buffer) or buffer[position] != ',':
                raise Outcome('error', 'array_separator_missing')
            position += 1; whitespace()
            if position < len(buffer) and buffer[position] == ']':
                raise Outcome('error', 'array_trailing_comma')
        while True:
            try:
                value, end = parser.raw_decode(buffer, position)
                break
            except json.JSONDecodeError:
                if eof:
                    raise Outcome('error', 'invalid_or_incomplete_array') from None
                if len(buffer[position:].encode('utf-8')) > maximum_record_bytes:
                    raise Outcome('error', 'json_record_byte_bound')
                more()
        if len(buffer[position:end].encode('utf-8')) > maximum_record_bytes:
            raise Outcome('error', 'json_record_byte_bound')
        position = end; first = False
        yield value
    whitespace()
    if position < len(buffer) or not eof:
        # whitespace() reads to EOF when there is no nonwhitespace remainder.
        raise Outcome('error', 'trailing_json_data')


def list_hash(dialogues):
    if type(dialogues) is not list or any(type(x) is not str for x in dialogues):
        raise Outcome('error', 'expected_string_list')
    return sha_bytes(canonical(dialogues).encode('utf-8', errors='strict'))


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def official_url(url, service, api=False):
    try:
        p = urlsplit(url)
        if p.scheme != 'https' or p.username or p.password or p.port not in (443, None):
            return False
        host = p.hostname or ''
        if service == 'opensubtitles':
            if api:
                return host in ('api.opensubtitles.com', 'vip-api.opensubtitles.com') and p.path.startswith('/api/v1/')
            return host in ('opensubtitles.com', 'opensubtitles.org') or host.endswith(('.opensubtitles.com', '.opensubtitles.org'))
        if service == 'archive':
            return (host == 'archive.org' or bool(re.fullmatch(r'(?:ia|dn)[0-9a-z.-]+\.archive\.org', host)))
    except ValueError:
        pass
    return False


class Network:
    """Cumulative request and transfer ledger, including retries and partial data.

    Raising the explicit limits permits another bounded resume. Login tokens and
    temporary download links are never checkpointed. POSTs are not automatically
    replayed after uncertain failures, since a download request may consume quota.
    """
    def __init__(self, work, service, max_requests=64, max_bytes=64*MIB,
                 retries=2, max_wait=30, timeout=20, opener=None, sleeper=time.sleep):
        if max_requests < 1 or max_bytes < 1 or not 0 <= retries <= 5 or not 0 <= max_wait <= 60:
            raise Outcome('error', 'invalid_network_bounds')
        self.work, self.service = Path(work), service
        self.path = self.work / ('network-' + service + '.json')
        self.state = load(self.path) if self.path.exists() else {'requests': 0, 'bytes': 0, 'resume_after': 0}
        self.max_requests, self.max_bytes = max_requests, max_bytes
        self.retries, self.max_wait, self.timeout = retries, max_wait, timeout
        self.opener = opener or build_opener(NoRedirect())
        self.sleeper = sleeper; self.last_request = 0

    def save(self):
        atomic_json(self.path, self.state)

    def wait(self, seconds):
        if seconds > self.max_wait:
            self.state['resume_after'] = time.time() + seconds
            self.save()
            raise Outcome('error', 'rate_limited_resume_later')
        self.sleeper(max(0, seconds))

    def reserve(self):
        remaining_wait = self.state.get('resume_after', 0) - time.time()
        if remaining_wait > 0:
            self.wait(remaining_wait)
        if self.state['requests'] >= self.max_requests or self.state['bytes'] >= self.max_bytes:
            raise Outcome('error', 'cumulative_network_bound_reached')
        if self.service == 'opensubtitles':
            self.wait(max(0, 1 - (time.monotonic() - self.last_request)))
        self.last_request = time.monotonic()
        self.state['requests'] += 1
        self.save()

    def consume(self, n):
        self.state['bytes'] += n
        self.save()
        if self.state['bytes'] > self.max_bytes:
            raise Outcome('error', 'cumulative_network_byte_bound')

    def retry_delay(self, headers, attempt):
        value = headers.get('Retry-After')
        if value:
            try:
                return max(0, float(value))
            except ValueError:
                try:
                    return max(0, parsedate_to_datetime(value).timestamp() - time.time())
                except (TypeError, ValueError):
                    return 60
        return min(2 ** attempt, 8)

    def rate_headers(self, headers):
        try:
            exhausted = any(float(headers.get(k, '1')) <= 0 for k in
                            ('X-RateLimit-Remaining-Second', 'X-RateLimit-Remaining-Minute', 'RateLimit-Remaining'))
            if exhausted:
                delay = self.retry_delay(headers, 0)
                if headers.get('X-RateLimit-Remaining-Minute') == '0':
                    delay = max(delay, 60)
                if headers.get('RateLimit-Remaining') == '0':
                    delay = max(delay, float(headers.get('RateLimit-Reset', '1')))
                self.state['resume_after'] = time.time() + delay
                self.save()
        except (ValueError, TypeError):
            raise Outcome('error', 'invalid_rate_limit_headers') from None

    def open(self, url, headers=None, body=None, api=False):
        headers = dict(headers or {})
        headers.update({'User-Agent': USER_AGENT, 'Accept-Encoding': 'identity'})
        redirects = 0; attempt = 0
        while True:
            if not official_url(url, self.service, api):
                raise Outcome('error', 'nonofficial_provider_url')
            self.reserve()
            try:
                response = self.opener.open(Request(url, data=body, headers=headers), timeout=self.timeout)
                if response.headers.get('Content-Encoding', 'identity').lower() not in ('identity', ''):
                    response.close()
                    raise Outcome('error', 'unexpected_http_content_encoding')
                self.rate_headers(response.headers)
                return response
            except HTTPError as error:
                status = error.code
                location = error.headers.get('Location')
                delay = self.retry_delay(error.headers, attempt)
                error.close()
                if status in (301, 302, 303, 307, 308) and location and redirects < 3:
                    target = urljoin(url, location)
                    if not official_url(target, self.service, api) or (api and urlsplit(target).netloc != urlsplit(url).netloc):
                        raise Outcome('error', 'unsafe_provider_redirect') from None
                    url = target; redirects += 1
                    continue
                if status in (401, 403):
                    raise Outcome('error', 'authentication_or_access_denied') from None
                if status in (404, 410):
                    raise Outcome('unavailable', 'provider_source_unavailable') from None
                if status in (402, 406):
                    raise Outcome('unavailable', 'provider_quota_or_download_limit') from None
                if body is None and (status == 429 or 500 <= status < 600) and attempt < self.retries:
                    self.wait(delay); attempt += 1; continue
                if status == 429:
                    self.state['resume_after'] = time.time() + delay; self.save()
                raise Outcome('error', 'http_' + str(status)) from None
            except (URLError, TimeoutError, OSError, http.client.HTTPException):
                if body is None and attempt < self.retries:
                    self.wait(min(2 ** attempt, 8)); attempt += 1; continue
                raise Outcome('error', 'network_failure_post_not_replayed' if body is not None else 'network_failure') from None

    def json(self, url, headers=None, body=None, limit=2*MIB, api=False):
        payload = canonical(body).encode('utf-8') if body is not None else None
        request_headers = dict(headers or {})
        request_headers.update({'Accept': 'application/json', 'Content-Type': 'application/json'})
        try:
            with self.open(url, request_headers, payload, api) as response:
                capacity = min(limit, self.max_bytes - self.state['bytes'])
                declared = response.headers.get('Content-Length')
                declared = int(declared) if declared is not None else None
                if declared is not None and not 0 <= declared <= capacity:
                    raise Outcome('error', 'api_response_declared_byte_bound')
                try:
                    raw = response.read(capacity)
                except http.client.IncompleteRead as interrupted:
                    self.consume(len(interrupted.partial))
                    raise Outcome('error', 'interrupted_api_response') from None
                self.consume(len(raw))
                if declared is None and len(raw) == capacity:
                    raise Outcome('error', 'api_response_bound_eof_unproven')
                if declared is not None and len(raw) != declared:
                    raise Outcome('error', 'interrupted_api_response')
            return parse(raw.decode('utf-8', errors='strict'))
        except (UnicodeError, ValueError, KeyError, http.client.HTTPException, OSError):
            raise Outcome('error', 'invalid_or_interrupted_api_response') from None

    def download(self, url, destination, identity, max_file_bytes, expected=None):
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        receipt_path = destination.with_name(destination.name + '.receipt.json')
        expected = expected or {}
        if expected.get('size') is not None and int(expected['size']) > max_file_bytes:
            raise Outcome('error', 'declared_file_exceeds_download_bound')
        if destination.exists():
            if not receipt_path.exists():
                raise Outcome('error', 'unregistered_cached_file')
            receipt = load(receipt_path)
            if receipt['identity'] != identity or receipt['sha256'] != sha_file(destination):
                raise Outcome('error', 'cached_file_integrity_mismatch')
            self.check_download(destination, expected, max_file_bytes)
            return receipt
        partial = destination.with_name(destination.name + '.part')
        partial_meta = partial.with_name(partial.name + '.json')
        if partial.exists() and (not partial_meta.exists() or load(partial_meta)['identity'] != identity):
            raise Outcome('error', 'partial_download_identity_mismatch')
        atomic_json(partial_meta, {'identity': identity, 'expected': expected})
        # Recover a crash after the complete receipt was committed but before
        # the final rename, without spending another request/download quota.
        if partial.exists() and receipt_path.exists():
            receipt = load(receipt_path)
            if receipt.get('identity') == identity and receipt.get('sha256') == sha_file(partial):
                self.check_download(partial, expected, max_file_bytes)
                os.replace(partial, destination)
                partial_meta.unlink(missing_ok=True)
                return receipt
        for attempt in range(self.retries + 1):
            offset = partial.stat().st_size if partial.exists() else 0
            headers = {'Range': 'bytes=' + str(offset) + '-'} if offset else {}
            try:
                with self.open(url, headers=headers) as response:
                    status = getattr(response, 'status', 200)
                    if offset and status == 206:
                        cr = response.headers.get('Content-Range', '')
                        if not cr.startswith('bytes ' + str(offset) + '-'):
                            raise Outcome('error', 'invalid_resume_content_range')
                    elif status == 200:
                        offset = 0  # Server ignored Range: restart, never append a full response.
                    elif status != 206 or offset:
                        raise Outcome('error', 'unsupported_download_status')
                    declared = response.headers.get('Content-Length')
                    declared = int(declared) if declared is not None else None
                    if declared is not None and (declared < 0 or offset + declared > max_file_bytes):
                        raise Outcome('error', 'file_byte_bound')
                    if declared is not None and declared > self.max_bytes - self.state['bytes']:
                        raise Outcome('error', 'cumulative_network_byte_bound')
                    transferred = 0
                    with partial.open('ab' if offset else 'wb') as stream:
                        os.chmod(partial, 0o600)
                        while True:
                            capacity = min(max_file_bytes - offset - transferred,
                                           self.max_bytes - self.state['bytes'], MIB)
                            if capacity <= 0:
                                if declared is not None and transferred == declared:
                                    break
                                raise Outcome('error', 'download_byte_bound_eof_unproven')
                            block = response.read(capacity)
                            if not block:
                                break
                            stream.write(block); stream.flush()
                            transferred += len(block); self.consume(len(block))
                    if declared is not None and transferred != declared:
                        raise OSError('interrupted')
                self.check_download(partial, expected, max_file_bytes)
                receipt = {'identity': identity, 'size': partial.stat().st_size, 'sha256': sha_file(partial)}
                # The receipt precedes rename; a crash cannot produce an unregistered complete cache.
                atomic_json(receipt_path, receipt)
                os.replace(partial, destination)
                partial_meta.unlink(missing_ok=True)
                return receipt
            except (URLError, TimeoutError, OSError, http.client.HTTPException):
                if attempt == self.retries:
                    raise Outcome('error', 'interrupted_download_resume_available') from None
                self.wait(min(2 ** attempt, 8))
        raise Outcome('error', 'download_failed')

    @staticmethod
    def check_download(path, expected, maximum):
        size = Path(path).stat().st_size
        if size > maximum or ('size' in expected and size != int(expected['size'])):
            raise Outcome('error', 'download_size_mismatch_or_bound')
        for key in ('sha256', 'sha1', 'md5'):
            if expected.get(key) and sha_file(path, key) != expected[key]:
                raise Outcome('mismatch', 'download_' + key + '_mismatch')


def add_network_arguments(parser, default_bytes=64*MIB):
    parser.add_argument('--max-requests', type=int, default=64, help='Cumulative private-workspace request ceiling, including retries.')
    parser.add_argument('--max-total-bytes', type=int, default=default_bytes, help='Cumulative transferred-byte ceiling, including failed attempts.')
    parser.add_argument('--max-file-bytes', type=int, default=16*MIB)
    parser.add_argument('--retries', type=int, default=2)


def cli_error(error):
    # Never print arbitrary exception messages, source paths, payloads or URLs.
    if isinstance(error, Outcome):
        return {'outcome': error.outcome, 'reason': error.reason}
    return {'outcome': 'error', 'reason': 'invalid_input_or_local_io_error'}
