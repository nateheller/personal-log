"""Explicit synchronous worker entry point, independent of recorder/UI processes."""

import sqlite3
from uuid import uuid4

from personal_log.storage.files import artifact_path
from .contracts import EngineInterrupted, JobState, LostClaim, RetryableError, SegmentText, TerminalError
from .sources import validate_file, validate_stream


class TranscriptionWorker:
    def __init__(self, store, engine, *, owner=None):
        self.store, self.engine, self.owner = store, engine, owner or str(uuid4())

    def run_one(self):
        """Return a durable state, None if idle, or lost_claim if fenced out.

        DB/I/O publication errors propagate with no success claim; the durable
        running attempt is later reconciled after lease expiry. No auto-loop.
        """
        store = self.store
        store.recover()
        store.enqueue(self.engine.identity)
        claim = store.claim(self.engine.identity, self.owner)
        if claim is None:
            return None

        def heartbeat():
            store.renew(claim)

        try:
            document, sources = store.snapshot(claim)
            for source in sources:
                heartbeat()
                validate_file(store.root, source)
            transcript_id = store.prepare_original(claim, document, sources)
            for source in sources:
                heartbeat()
                if store.has_result(transcript_id, source):
                    continue
                validate_file(store.root, source)
                store.start_segment(claim, transcript_id, source)
                try:
                    with artifact_path(store.root, source.relative_path).open('rb') as audio:
                        validate_stream(audio, source)
                        try:
                            result = self.engine.transcribe(audio, source, heartbeat)
                        except (RetryableError, TerminalError, EngineInterrupted, LostClaim, sqlite3.Error):
                            raise
                        except Exception:
                            raise TerminalError('engine_exception') from None
                        if not isinstance(result, SegmentText):
                            raise TerminalError('invalid_engine_result')
                        heartbeat()
                        validate_stream(audio, source)
                except (OSError, ValueError):
                    raise TerminalError('source_audio_invalid') from None
                validate_file(store.root, source)
                store.publish(claim, transcript_id, source, result, document)
            # Never complete from a cached usable subset or cached successes.
            for source in sources:
                heartbeat()
                validate_file(store.root, source)
            store.complete(claim, transcript_id, document)
            return JobState.COMPLETED
        except LostClaim:
            return 'lost_claim'
        except (RetryableError, TerminalError, EngineInterrupted) as error:
            target = JobState.RETRYABLE if isinstance(error, RetryableError) else (
                JobState.INTERRUPTED if isinstance(error, EngineInterrupted) else JobState.TERMINAL)
            try:
                store.fail(claim, target, error.code)
            except LostClaim:
                return 'lost_claim'
            return JobState(store.db.execute('SELECT state FROM transcription_jobs WHERE job_id=?', (claim.job_id,)).fetchone()[0])
        except (KeyboardInterrupt, SystemExit):
            try:
                store.fail(claim, JobState.INTERRUPTED, 'worker_interrupted')
            except LostClaim:
                pass
            raise
