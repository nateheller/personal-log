# Windows microphone adapter and manual qualification

**WARNING: Audio and metadata are UNENCRYPTED. Record disposable test audio only.
Do not speak personal or sensitive content. Nothing here qualifies this prototype
for private journals. All audio and databases must remain outside the checkout.**

## Architecture and format

`recording/microphone.py` provides a sounddevice/PortAudio adapter for the existing
`Capture` protocol in `fake.py`. The synthetic source remains the default.
`RecorderConfig(capture_source='microphone', input_device=..., input_device_name=...,
input_host_api=...)` explicitly opts in. These are host configuration, not new IPC
fields. No device access occurs on import or recorder startup/recovery. Record
starts the selected stream inside the existing worker, after session creation.
Resume revalidates and reopens that same identity. No default-device fallback,
resampling, automatic microphone switching, or automatic restart is implemented.

Audio uses the existing mono, signed little-endian PCM16 storage format. Windows
is little-endian. The default is 48 kHz; discovery probes mono PCM16 at 16, 44.1,
48 kHz and the device default rate. This is a supported-rate probe list, not an
exhaustive device capability matrix. A stereo-only device without a supported
mono configuration is rejected; no implicit downmix occurs. WASAPI inputs are
listed first, but selection is explicit and other supported host APIs are allowed.
The stream uses the selected API's default mode (WASAPI shared mode), high input
latency, and a fixed block size (default 100 ms). No exclusive-mode requirement.

The optional `microphone` dependency is `sounddevice>=0.5.1,<0.6`. RawInputStream
uses buffer objects without requiring NumPy. Its callback only counts frames,
checks flags/size, and copies bounded PCM blocks into a bounded deque. No file,
database, hashing, WAV, IPC, logging, or UI operations run there. CPython's GIL and
single-producer/single-consumer deque operations are relied upon; this is not a
lock-free native ring buffer or a hard real-time scheduling guarantee. At most
`queue_blocks` callback blocks are retained, each at most `block_seconds` of PCM,
plus one callback copy and the existing bounded storage FIFO. A larger unexpected
callback block fails closed rather than growing the buffer.

The worker consumes this buffer without waiting for audio, retaining IPC/liveness
ticks even when it is empty. Audio follows the same storage FIFO, SegmentStore,
SQLite migrations, segment publication, hashes, recovery, and exactly-once job
enqueue path as synthetic capture. No new schema or IPC version is necessary.
The existing interval device_id stores a JSON selection descriptor (PortAudio
index, name, host API) for real inputs; Resume preserves it. Synthetic intervals
retain their existing synthetic identifier.

Pause/Finish stop and close the stream outside the callback, join callback activity,
and drain its buffered prefix before the existing lifecycle barrier. Resume uses a
new capture interval; storage start_frame resets while the source's frame counter
continues. Interval timestamps are lifecycle timestamps, not calibrated hardware
ADC wall-clock measurements. Rapid operations remain serialized and deduplicated.
Stream start can fail after Record/Resume metadata committed; that operation's
live reply is rejected with capture_failed and the session is interrupted. An old
durable receipt remains historical metadata, as in Increment 3A, and never restarts
capture when replayed. A host must use live status to assess current recording.

## Conservative failure handling

Callback overflow/status errors, malformed input, callback exceptions, buffer
saturation, unexpected stream completion, inactive handles, status-query errors,
and two seconds without a callback latch a failure. Stop/close failures latch too.
Device names/API and availability are checked at every open; IDs are ephemeral
PortAudio enumeration indexes, not persistent Windows device GUIDs. Same-name
replacement and driver-internal rerouting cannot be authenticated by this adapter.

Failure stops capture, drains what can be accepted without blocking, and prevents
Finish or normal shutdown from committing successful completion or a job. Known
callback frame counts include rejected frames; captured minus storage-accounted
frames persists as buffered_capture_gap when metadata remains writable. PortAudio
input overflow has no exact missing-frame count: it produces an uncertainty finding
as well as any measurable rejected callback frames. No silence is fabricated and
no lost samples are silently bridged into a successful recording.

Source failure details (sanitized codes such as input_overflow or initialization_failed)
are stored as recovery findings; IPC retains its existing capture_failed code.
Storage saturation may leave callback-buffer audio unpersisted; that known loss is
counted, capture stops, and completion is not acknowledged. Hardware/OS buffered
audio never delivered to the callback cannot be counted exactly. Hard termination
may prevent final gap accounting: startup recovery conservatively reports the
unknown capture tail. Existing supervisor deadlines remain authoritative even if
a PortAudio open/stop/close call hangs. Genuine driver stalls, Windows sleep and
shutdown, privacy permissions, hardware failures, and power cuts need manual
qualification; process-crash tests cannot establish those guarantees.

## Explicit PowerShell manual procedure

These commands are for a person to run deliberately. Automated tests never open a
microphone. Run from the checkout on `feature/recording-foundation`:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev,microphone]"
.\.venv\Scripts\python.exe -m personal_log.recording.cli devices
$testRoot = Join-Path $env:LOCALAPPDATA ("PersonalLog\MicTest-" + [guid]::NewGuid())
```

Inspect the JSON device names, host APIs, indexes, and supported_rates. Prefer the
intended WASAPI device if it supports mono PCM16; another listed API is allowed.
Enable Windows microphone access for desktop apps if required. Re-list after
connecting/disconnecting devices; indexes may change. Substitute the chosen index
and a listed rate below (7 and 48000 are examples, not assumed device settings):

```powershell
.\.venv\Scripts\python.exe -m personal_log.recording.cli record --device 7 --rate 48000 --root $testRoot --disposable-test-audio
```

The command prints the warning, external data root, worker PID, and Record reply.
It then accepts one command per line: `pause`, `resume`, `finish`, `interrupt`.
The console reader is separate from the main IPC/heartbeat loop; waiting at the
prompt does not let the lease expire. This is a manual test host, not desktop UI.

1. Speak disposable phrases such as “test one, two, three” for at least 25 seconds.
2. Enter `pause`. Confirm a paused acknowledgment, then make a distinguishable
   sound that should not appear in the recording.
3. Enter `resume`, speak another test phrase, then enter `finish` during speech.
4. Require an acknowledgment with state finished. Any exception/interruption or
   capture_failed reply is a failed test, even if some playable WAV files exist.
5. Locate files outside the checkout:

```powershell
Get-ChildItem -LiteralPath (Join-Path $testRoot 'audio') -Filter '*.wav'
```

Use Windows Explorer/audio player to listen to the finalized WAV files. Segment
UUID filenames do not encode ordering; SQLite audio_segments.sequence does.
To print validated effective paths and frame counts in sequence order:

```powershell
$env:PERSONAL_LOG_TEST_ROOT = $testRoot
@'
import os
from pathlib import Path
from personal_log.storage.segments import SegmentStore
with SegmentStore(repository=Path.cwd(), root=Path(os.environ['PERSONAL_LOG_TEST_ROOT'])) as store:
    for row in store.db.execute('SELECT session_id,state FROM recording_sessions ORDER BY created_at'):
        print(dict(row))
        for segment in store.segments(row['session_id']):
            print(dict(segment))
'@ | .\.venv\Scripts\python.exe -
```

Verify audible content, natural speed/pitch, no duplicated words at segment/interval
boundaries, the omitted pause sound, and the expected ending. A listening test is
not proof of sample-perfect capture. Keep this directory until review is complete;
no retention or automatic cleanup is implemented.

For a safe interruption test, use a fresh external root, start another disposable
recording, speak briefly, then enter `interrupt`. Expect interrupted, not finished.
Run recovery after the host exits:

```powershell
.\.venv\Scripts\python.exe -m personal_log.recording.cli recover --root $testRoot
.\.venv\Scripts\python.exe -m personal_log.recording.cli recover --root $testRoot
```

Repeated recovery must not add duplicate repairs, findings, or sessions. It must
not reopen a microphone. To simulate a worker crash deliberately, use a separate
PowerShell window and `Stop-Process -Id <printed-worker-pid> -Force` while recording
disposable audio in a fresh root. Verify the controller reports failure, exits,
and that the same recovery commands preserve available segments. Do not terminate
arbitrary Python processes or delete artifacts. To test unplugging, unplug only
the selected removable test microphone while recording; expect interruption, no
automatic fallback, and no successful Finish/job. Stop using the prototype if the
capture indicator remains active after the approved shutdown deadline.

## Automated verification and review

Run `.venv/Scripts/python.exe -m pytest -q`. `tests/test_microphone.py` uses only a
simulated backend and synthetic callback PCM, including spawned worker/supervisor
integration. It covers enumeration/selection, unsupported formats/init failures,
identity changes, ordering, saturation, overflow, malformed callbacks, disconnect,
status errors, absent callbacks, rapid Pause/Resume, stop-time callback tails and
overflow, storage failure/backpressure, worker crashes, supervisor death, shutdown,
startup recovery, and console terminal-command backpressure. Existing synthetic,
path-safety, schema, durability, IPC, and theme tests remain unchanged.

Independent review identified and corrected suppressed ticks on empty reads,
uncaught backend status-query exceptions, and lost console terminal commands on a
full queue. No real device, dependency installation, or hardware microphone test
was performed by development agents. Optional dependency installation and native
PortAudio/WASAPI behavior still require the explicit manual procedure above.

References: [sounddevice raw streams](https://python-sounddevice.readthedocs.io/en/latest/api/raw-streams.html),
[hardware discovery/settings](https://python-sounddevice.readthedocs.io/en/latest/api/checking-hardware.html),
and [callback restrictions](https://python-sounddevice.readthedocs.io/en/latest/api/streams.html).
