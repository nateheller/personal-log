# Architecture decisions

- ADR-001: local-first native Windows desktop app, Python + PySide6 candidate.
- ADR-002: recording independent of AI inference; durable segmented capture.
- ADR-003: raw and refined text stored separately with versions.
- ADR-004: UI themes are data-driven and isolated from core.
- ADR-005: local AI analysis is optional and non-destructive.
- ADR-006: begin with a private repo pending explicit publishing decision.
- ADR-007: journaling and task management are independently usable local-first
  functions. Tasks are first-class records and never require a journal reference.
- ADR-008: SQLite is authoritative for confirmed task state. Manual edits and
  explicit user decisions take precedence over analysis suggestions; changes have
  an auditable history. Core task operations remain available offline.
- ADR-009: journal-derived task discoveries and updates are separate suggestions
  until confirmed. Preserve source/version/passage provenance and duplicate checks;
  never modify source journal text or silently reopen completed/dismissed tasks.
- ADR-010: reminders and daily-overview delivery have state separate from tasks.
  Email is optional, explicitly enabled, and subject to content privacy controls;
  delivery failure cannot change task status. The in-app overview uses SQLite and
  works without journals, AI, or email.

ADR-007 through ADR-010 record approved product direction for future work, not
implemented capabilities. See [ROADMAP.md](ROADMAP.md) and
[TASK_MANAGEMENT_PLAN.md](TASK_MANAGEMENT_PLAN.md). No task schema, migration,
runtime service, or UI is introduced by these planning decisions.
