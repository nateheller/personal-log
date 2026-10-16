"""Explicit, disposable-data manual capture host; never runs on import."""

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import queue
import threading
import time

from .contracts import DISPOSABLE_WARNING
from .microphone import discover_inputs
from .process import RecorderConfig, RecorderProcess
from personal_log.storage.segments import SegmentStore


def _read_commands(lines, read_line=input):
    while True:
        try:
            line = read_line().strip().lower()
        except EOFError:
            line = 'interrupt'
        if line in {'finish', 'interrupt'}:
            # Waiting is safe on this console-only thread; never lose its final
            # control command and leave the main thread renewing the lease.
            lines.put(line)
            return
        try:
            lines.put_nowait(line)
        except queue.Full:
            pass


def main(argv=None):
    parser = argparse.ArgumentParser(description=DISPOSABLE_WARNING)
    parser.add_argument('action', choices=['devices', 'record', 'recover'])
    parser.add_argument('--repository', type=Path, default=Path.cwd())
    parser.add_argument('--root', type=Path)
    parser.add_argument('--device', type=int)
    parser.add_argument('--rate', type=int, default=48000)
    parser.add_argument('--disposable-test-audio', action='store_true')
    args = parser.parse_args(argv)
    print(DISPOSABLE_WARNING, flush=True)
    if args.action == 'devices':
        print(json.dumps([asdict(device) for device in discover_inputs()], indent=2))
        return
    if args.action == 'recover':
        with SegmentStore(repository=args.repository, root=args.root) as store:
            print(json.dumps([dict(row) for row in store.recover()], indent=2))
            print('Audio directory:', store.root / 'audio')
        return
    if not args.disposable_test_audio or args.device is None:
        parser.error('record requires --device and --disposable-test-audio')
    devices = {device.index: device for device in discover_inputs()}
    device = devices.get(args.device)
    if device is None or args.rate not in device.supported_rates:
        parser.error('Selected input/rate unavailable; list devices again')
    settings = RecorderConfig(capture_source='microphone', input_device=device.index,
        input_device_name=device.name, input_host_api=device.host_api, sample_rate=args.rate)
    # Only this main thread uses IPC. Console input cannot stall its heartbeats.
    lines = queue.Queue(8)

    with RecorderProcess(repository=args.repository, root=args.root, config=settings) as recorder:
        print('Data root:', recorder.root, 'Worker PID:', recorder.descriptor['worker_pid'], flush=True)
        started = recorder.request('record')['payload']
        print(started, flush=True)
        if started['code'] != 'ack':
            return
        print('Commands: pause, resume, finish, interrupt (preserve without successful Finish)', flush=True)
        threading.Thread(target=_read_commands, args=(lines,), daemon=True).start()
        next_heartbeat = time.monotonic()
        while recorder.process.is_alive():
            if time.monotonic() >= next_heartbeat:
                recorder.send(recorder.make_command('heartbeat'))
                next_heartbeat = time.monotonic() + 1
            for event in recorder.poll():
                if event['payload']['code'] == 'interrupted':
                    print(event['payload'], flush=True)
                    return
            try:
                line = lines.get_nowait()
            except queue.Empty:
                time.sleep(0.02)
                continue
            if line in {'pause', 'resume', 'finish'}:
                response = recorder.request(line)
                print(response['payload'], flush=True)
                if line == 'finish' and response['payload']['state'] == 'finished':
                    print('Finalized WAV files are in:', recorder.root / 'audio', flush=True)
                    return
            elif line == 'interrupt':
                print(recorder.request('shutdown', reason='ui_crash')['payload'], flush=True)
                return


if __name__ == '__main__':
    main()
