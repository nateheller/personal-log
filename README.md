# Personal Log

Private, local-first Windows voice journal. **Voice is temporary. Your words are permanent.**

This is an early architecture scaffold, **not a secure journaling application**. Do not record real sensitive content until capture, encryption, and recovery are tested.

**WARNING: Recordings are UNENCRYPTED. Use disposable test data only. Do not record personal or sensitive content.**

## Initial milestone
Build crash-resistant segmented recording with synthetic/disposable audio and automated recovery tests.

## Setup
`python -m pip install -e ".[dev]"` then `pytest`.

See `docs/ROADMAP.md` and `AGENTS.md`.

Increment 1 contracts, shutdown policy, and limitations: `docs/MILESTONE_1_INCREMENT_1.md`.
