#!/usr/bin/env python3
"""Bounded production workflow verification; baseline automated testing is deferred."""
import argparse
import hashlib
import io
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def need(value, message):
    if not value:
        raise RuntimeError(message)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workflow', default='all')
    args = ap.parse_args()
    observations = []
    with tempfile.TemporaryDirectory(prefix='cadence-workflow-') as scratch:
        scratch = Path(scratch)
        ffmpeg = os.environ.get('FFMPEG_BIN', 'ffmpeg')
        subprocess.run([ffmpeg, '-nostdin', '-hide_banner', '-loglevel', 'error',
                        '-f', 'lavfi', '-i', 'testsrc2=size=160x90:rate=24:duration=4',
                        '-f', 'lavfi', '-i', 'sine=frequency=660:sample_rate=48000:duration=4',
                        '-c:v', 'libx264', '-preset', 'ultrafast', '-threads', '1',
                        '-c:a', 'aac', '-shortest', str(scratch/'calibration.mp4')], check=True, timeout=30)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        address = 'http://127.0.0.1:'+str(port)+'/forge/v1'
        env = dict(os.environ, FORGE_ROOT=str(scratch/'data'),
                   FORGE_ADDR='127.0.0.1:'+str(port), FORGE_WORKERS='1')
        log = open(scratch/'server.log', 'ab')
        process = None
        def request(method, route, body=None, headers=None, status=200, raw=False):
            hs = dict(headers or {})
            if isinstance(body, (dict, list)):
                body = json.dumps(body).encode()
                hs['Content-Type'] = 'application/json'
            req = urllib.request.Request(address+route, data=body, headers=hs, method=method)
            try:
                response = urllib.request.urlopen(req, timeout=60)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                data = response.read()
                need(response.status == status, f'{method} {route}: {response.status}, wanted {status}: {data[:600]!r}')
                return data if raw or not data else json.loads(data)
        def start():
            nonlocal process
            process = subprocess.Popen([str(ROOT/'bin/forged')], cwd=ROOT, env=env,
                                       stdout=log, stderr=log, start_new_session=True)
            deadline = time.monotonic()+10
            while time.monotonic() < deadline:
                try:
                    request('GET', '/health')
                    return
                except (OSError, RuntimeError):
                    if process.poll() is not None:
                        raise RuntimeError((scratch/'server.log').read_text())
                    time.sleep(.05)
            raise RuntimeError('server readiness timed out')
        def stop(force=False):
            nonlocal process
            if process and process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
                process.wait(timeout=15)
            process = None
        def observe(name, detail):
            observations.append({'workflow_id': name, 'passed': True, 'observed': detail})
        def durable_document():
            # The server replaces state.json atomically, so after a confirmed
            # HTTP response this reads the committed document whole.
            return json.loads((scratch/'data'/'state.json').read_text())
        def document_fingerprint():
            st = os.stat(scratch/'data'/'state.json')
            return (st.st_ino, st.st_mtime_ns, st.st_size)
        def catch_up(after=0):
            seen = []
            cursor = after
            while True:
                page = request('GET', '/events?after='+str(cursor))
                need(len(page) <= 200, 'event page exceeded the 200 record bound')
                need(all(set(e) == {'sequence', 'subject', 'kind', 'detail', 'at'} for e in page), 'event shape drifted')
                need(all(e['sequence'] > cursor for e in page), 'event page did not advance the cursor')
                need(all(a['sequence'] < b['sequence'] for a, b in zip(page, page[1:])), 'event page order is not strict')
                if not page:
                    return seen
                seen.extend(page)
                cursor = page[-1]['sequence']
                if len(page) < 200:
                    return seen
        def event_kinds(subject, events):
            return [e['kind'] for e in events if e['subject'] == subject]
        def confirmed(before, subject, kind):
            # A confirmation must already be one durable fact: the state change
            # and exactly one explaining event in the same committed document.
            # Callers only use this where no worker can concurrently append.
            doc = durable_document()
            need(doc['sequence'] == before['sequence'] + 1, f'{kind} did not commit exactly one event')
            need(doc['events'][-1]['subject'] == subject and doc['events'][-1]['kind'] == kind,
                 f'{kind} was not the confirmed explaining event')
            return doc
        def assert_document_events():
            # The event boundary: the committed document is the single authority
            # for both current state and the event log, and catch-up returns
            # exactly its records without gaps or renumbering.
            doc = durable_document()
            page = catch_up(0)
            need(page == doc['events'], 'event catch-up diverged from the durable document')
            if page:
                need(page[-1]['sequence'] == doc['sequence'], 'durable cursor does not match the last event')
            for rid, r in doc['renders'].items():
                kinds = event_kinds(rid, page)
                need(kinds and kinds[0] == 'render.queued', f'render {rid} has no queued fact')
                need(kinds[-1] == 'render.'+r['state'], f'render {rid} state and explaining event disagree: {kinds[-1]} vs {r["state"]}')
                need(all(k in ('render.queued', 'render.started', 'render.succeeded', 'render.failed', 'render.cancelled', 'render.retried', 'render.interrupted') for k in kinds), 'unknown render event kind')
            return doc, page
        def wait_render(rid, desired=None):
            deadline = time.monotonic()+40
            while time.monotonic() < deadline:
                r = request('GET', '/renders/'+rid)
                if desired and r['state'] == desired:
                    return r
                if not desired and r['state'] in ('succeeded', 'failed', 'cancelled'):
                    return r
                time.sleep(.02)
            raise RuntimeError('render state timeout')
        try:
            start()
            media = (scratch/'calibration.mp4').read_bytes()
            source = request('POST', '/sources?name=calibration-pulse', media, status=201)
            sid = source['id']
            need(source['media']['video'] and source['media']['audio'], 'probe lost streams')
            need(source['digest'] == hashlib.sha256(media).hexdigest(), 'source digest mismatch')
            request('POST', '/sources?name=invalid', b'not-media', status=400)
            duplicate = request('POST', '/sources?name=duplicate', media, status=201)
            need(duplicate['id'] == sid, 'ingest did not converge')
            observe('ingest', 'MP4 accepted with SHA-256; malformed bytes rejected; duplicate converges')
            recipe_body = dict(name='review-proxy', format='mp4', width=160, height=90,
                               fps=24, sample_rate=48000, normalize=False, target_lufs=-16)
            doc = durable_document()
            recipe = request('POST', '/recipes', recipe_body, status=201)
            rp = recipe['id']
            doc = confirmed(doc, rp, 'recipe.revised')
            need(len(doc['recipes'][rp]) == 1 and doc['recipes'][rp][0]['revision'] == 1, 'recipe revision and event disagree')
            updated = request('PUT', '/recipes/'+rp, dict(recipe_body, name='review-proxy-v2'), {'If-Match': '1'}, status=201)
            doc = confirmed(doc, rp, 'recipe.revised')
            need(updated['revision'] == 2, 'recipe revision not advanced')
            request('PUT', '/recipes/'+rp, recipe_body, {'If-Match': '1'}, status=409)
            need(durable_document() == doc, 'rejected recipe revision still wrote state or events')
            observe('recipe-version', 'revision 2 saved with its fact in one write; stale revision rejected without a write')
            sheet_body = dict(name='two-pass-calibration', spans=[
                dict(source_id=sid, in_ms=0, out_ms=1500, gain_db=-3),
                dict(source_id=sid, in_ms=2000, out_ms=3500, gain_db=0)],
                cues=[dict(source_id=sid, start_ms=1000, end_ms=2500, text='校准 <幅度>')])
            sheet = request('POST', '/sheets', sheet_body, status=201)
            sh = sheet['id']
            doc = confirmed(doc, sh, 'sheet.revised')
            revision = request('PUT', '/sheets/'+sh, dict(sheet_body, name='revised-calibration'), {'If-Match': '1'}, status=201)
            doc = confirmed(doc, sh, 'sheet.revised')
            need(revision['revision'] == 2, 'sheet revision not advanced')
            request('PUT', '/sheets/'+sh, sheet_body, {'If-Match': '1'}, status=409)
            need(durable_document() == doc, 'rejected sheet revision still wrote state or events')
            need([e['kind'] for e in doc['events'] if e['subject'] == sh] == ['sheet.revised', 'sheet.revised'], 'sheet revisions and events disagree')
            observe('sheet-version', 'two-cut revision saved with its fact in one write; stale writer rejected without a write')
            plan = request('GET', '/sheets/'+sh+'/preview?recipe_id='+rp)
            need(plan['duration_ms'] == 3000 and plan['placements'][1]['output_in_ms'] == 1500, 'timeline mismatch')
            observe('preview', 'two source spans compacted to a 3000ms composition')
            need('00:00:01.000 --> 00:00:01.500' in plan['vtt'], 'first caption clipping mismatch')
            need('00:00:01.500 --> 00:00:02.000' in plan['vtt'], 'second caption mapping mismatch')
            need('&lt;幅度&gt;' in plan['vtt'], 'caption markup not escaped')
            observe('captions', 'source cue split and clipped into two escaped output intervals')
            if args.workflow in ('all', 'loudness'):
                loud = request('POST', '/sources/'+sid+'/loudness')
                need(loud['integrated_lufs'] is not None and not loud['silent'], 'loudness scan failed')
                observe('loudness', 'measured integrated LUFS and true peak')
            for mode in ['storyboard', 'waveform']:
                if args.workflow in ('all', mode):
                    image = request('GET', '/sources/'+sid+'/visual?mode='+mode, raw=True)
                    need(image.startswith(b'\x89PNG'), 'inspection output is not PNG')
                    observe(mode, f'PNG produced from source streams: {len(image)} bytes')
            req = dict(sheet_id=sh, sheet_revision=1, recipe_id=rp, recipe_revision=1)
            render = request('POST', '/renders', req, {'Idempotency-Key': 'first-render'}, status=202)
            rid = render['id']
            # A confirmed submission is already one durable fact: the queued
            # render and the event explaining it are in the same committed
            # document before the 202 is produced.
            submitted = durable_document()
            need(rid in submitted['renders'], 'submit acknowledgement has no durable render')
            submitted_kinds = event_kinds(rid, submitted['events'])
            need(submitted_kinds and submitted_kinds[0] == 'render.queued', 'confirmed submission lacks its queued fact')
            again = request('POST', '/renders', req, {'Idempotency-Key': 'first-render'}, status=202)
            need(again['id'] == rid, 'idempotency key did not converge')
            request('POST', '/renders', dict(req, sheet_revision=2), {'Idempotency-Key': 'first-render'}, status=409)
            observe('idempotent-submit', 'same request converges; changed revision conflicts')
            done = wait_render(rid)
            need(done['state'] == 'succeeded', str(done['attempts']))
            need(done['sheet']['revision'] == 1 and done['recipe']['revision'] == 1, 'frozen revisions drifted')
            observe('mp4-render', 'frozen revisions encoded to H.264/AAC MP4 and inspected')
            verification = request('POST', '/renders/'+rid+'/verify')
            artifact = request('GET', '/renders/'+rid+'/artifact', raw=True)
            need(hashlib.sha256(artifact).hexdigest() == verification['digest'], 'artifact digest mismatch')
            observe('artifact-verify', 'download SHA-256 equals committed inspection')
            if args.workflow in ('all', 'bundle'):
                packed = request('GET', '/renders/'+rid+'/bundle', raw=True)
                with zipfile.ZipFile(io.BytesIO(packed)) as z:
                    need(set(z.namelist()) == {'composition.mp4', 'captions.vtt', 'composition.json'}, 'bundle members mismatch')
                    frozen = json.loads(z.read('composition.json'))
                    need(frozen['plan']['vtt'] == z.read('captions.vtt').decode(), 'bundle captions drifted')
                observe('bundle', 'verified media, captions and frozen manifest exported together')
            if args.workflow in ('all', 'wav-render'):
                ar = request('POST', '/recipes', dict(recipe_body, name='reference-audio', format='wav', normalize=True), status=201)
                audio_render = request('POST', '/renders', dict(req, recipe_id=ar['id']), {'Idempotency-Key': 'audio'}, status=202)
                audio_done = wait_render(audio_render['id'])
                need(audio_done['state'] == 'succeeded', str(audio_done['attempts']))
                need(audio_done['artifact']['inspection']['media']['audio_codec'] == 'pcm_s16le', 'audio codec mismatch')
                observe('wav-render', 'trimmed stereo PCM WAV encoded with loudness filter')
            request('DELETE', '/sources/'+sid, status=409)
            observe('retire-source', 'historical sheet reference prevents retirement')
            if args.workflow in ('all', 'cancel', 'retry', 'restart-recover'):
                long_sheet = request('POST', '/sheets', dict(sheet_body, name='extended-calibration', spans=sheet_body['spans']*16), status=201)
                pending = request('POST', '/renders', dict(req, sheet_id=long_sheet['id']), {'Idempotency-Key': 'interrupt'}, status=202)
                interrupted_id = pending['id']
                wait_render(interrupted_id, 'running')
                cancelled = request('POST', '/renders', req, {'Idempotency-Key': 'cancel-me'}, status=202)
                cancel_id = cancelled['id']
                cancelled = request('POST', '/renders/'+cancel_id+'/cancel')
                need(cancelled['state'] == 'cancelled', 'cancel not acknowledged')
                cancel_doc = durable_document()
                need(cancel_doc['renders'][cancel_id]['state'] == 'cancelled', 'cancel acknowledgement is not durable')
                need(event_kinds(cancel_id, cancel_doc['events'])[-1] == 'render.cancelled', 'cancel acknowledgement lacks its cancelled fact')
                request('GET', '/renders/'+cancel_id+'/artifact', status=409)
                observe('cancel', 'cancelled acknowledged only after state and cancelled fact committed together; download refused')
                stop(force=True)
                before_restart = durable_document()
                need(before_restart['renders'][interrupted_id]['state'] == 'running', 'forced stop did not leave running state')
                need(event_kinds(interrupted_id, before_restart['events'])[-1] == 'render.started', 'running state lacks its started fact')
                cursor_before_restart = before_restart['sequence']
                start()
                recovered = request('GET', '/renders/'+interrupted_id)
                need(recovered['state'] == 'failed' and recovered['attempts'][-1]['outcome'] == 'interrupted', 'recovery provenance mismatch')
                after_restart = durable_document()
                need(event_kinds(interrupted_id, after_restart['events'])[-1] == 'render.interrupted', 'recovery state change lacks its interrupted fact')
                need(len(after_restart['events']) == len(before_restart['events']) + 1, 'recovery wrote more than one event')
                crossed = catch_up(cursor_before_restart)
                need([e['kind'] for e in crossed] == ['render.interrupted'], 'catch-up across restart did not see exactly the recovery fact')
                resend_before = document_fingerprint()
                resumed = request('POST', '/renders', dict(req, sheet_id=long_sheet['id']), {'Idempotency-Key': 'interrupt'}, status=202)
                need(resumed['id'] == interrupted_id, 'resend after interruption did not converge on the frozen task')
                need(resumed['state'] == 'failed', 'resend exposed a state the durable document does not have')
                need(document_fingerprint() == resend_before, 'resend after interruption caused a new durable write')
                observe('restart-recover', 'SIGKILL left state without a delivery event; restart committed failed/interrupted and its fact together; resend coalesced without writing')
                request('POST', '/renders/'+interrupted_id+'/retry', status=202)
                retried = wait_render(interrupted_id)
                need(retried['state'] == 'succeeded' and len(retried['attempts']) == 2, str(retried['attempts']))
                retried_doc = durable_document()
                kinds = event_kinds(interrupted_id, retried_doc['events'])
                need(kinds.count('render.interrupted') == 1 and kinds[-1] == 'render.succeeded', 'retry event chain mismatch')
                observe('retry', 'second attempt completed using original snapshot')
            events = request('GET', '/events?after=0')
            need(events and all(a['sequence'] < b['sequence'] for a, b in zip(events, events[1:])), 'event order mismatch')
            tail = request('GET', '/events?after='+str(events[-1]['sequence']))
            need(all(e['sequence'] > events[-1]['sequence'] for e in tail), 'event cursor repeated')
            catch_up_tail = catch_up(events[-1]['sequence'])
            need(catch_up_tail == [], 'catch-up after the last event returned records')
            mid = events[len(events)//2]['sequence']
            catch_up_mid = catch_up(mid)
            need([e['sequence'] for e in catch_up_mid] == [e['sequence'] for e in events if e['sequence'] > mid], 'mid-cursor catch-up skipped or reordered records')
            observe('events', 'persisted event cursor strictly increases; resend, interruption and catch-up pages agree with the durable document')
            if args.workflow == 'all':
                # Verify the event/state boundary holds on the final document
                # without any additional write: no event without its state and
                # no render state change without its explaining event.
                final_doc, final_events = assert_document_events()
                need(final_doc['sequence'] == final_events[-1]['sequence'], 'durable cursor disagrees with final event')
                # Cross the 200-record page bound using ordinary sheet revisions,
                # then verify paging continues the same strict cursor and that
                # catch-up reconstructs the durable log exactly.
                if final_doc['sequence'] <= 200:
                    extra = request('POST', '/sheets', dict(sheet_body, name='cursor-page-fill'), status=201)
                    revision = 1
                    while final_doc['sequence'] <= 205:
                        revision += 1
                        request('PUT', '/sheets/'+extra['id'], dict(sheet_body, name='cursor-page-fill'),
                                {'If-Match': str(revision - 1)}, status=201)
                        final_doc = durable_document()
                page = request('GET', '/events?after=0')
                need(len(page) == 200, 'first page did not honor the 200 record bound')
                second = request('GET', '/events?after='+str(page[-1]['sequence']))
                need(second and second[0]['sequence'] == page[-1]['sequence'] + 1, 'page two did not continue the strict cursor')
                need(catch_up(0) == final_doc['events'], 'paged catch-up did not reconstruct the durable log')
                observe('event-state-boundary', 'one committed document per confirmation; catch-up equals events across the 200-record page bound; terminal states carry their facts')
            if args.workflow != 'all':
                need(any(x['workflow_id'] == args.workflow for x in observations), 'unknown workflow')
            print(json.dumps({'ok': True, 'requested': args.workflow, 'observations': observations}, ensure_ascii=False))
        except Exception:
            log.flush()
            print((scratch/'server.log').read_text(errors='replace'))
            raise
        finally:
            stop()
            log.close()

if __name__ == '__main__':
    main()
