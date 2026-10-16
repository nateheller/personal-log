# Development roadmap

Personal Log has two complementary functions: personal journaling and personal
task management. Each must work independently. Tasks are first-class SQLite
records; journal analysis is an optional source of suggestions, never a prerequisite
for creating, organizing, completing, or reviewing tasks.

This roadmap describes planned work, not implemented functionality. This update
does not change ongoing transcription work or authorize task implementation.

## Journaling track

1. **Capture and recovery:** segmented durable audio, pause/resume, manifest, recovery tests, disposable audio only.
2. **Offline processing:** persistent idempotent queue, local ASR, conservative refinement, provenance.
3. **Security:** encryption at rest, PIN/unlock design, key recovery, verified backup/restore, seven-day post-success audio retention.
4. **Desktop experience:** PySide6, supplemental entries, chronological browser, FTS5, theme tokens.
5. **Optional insights:** semantic search, evidence-linked topics/timelines, uncertainty-aware analytics.

The existing capture, processing, security, and theme boundaries remain in effect.
Task planning does not reorder or expand the current transcription increment.

## Task-management track

1. **Standalone task foundation:** define task contracts and lifecycle; plan
   versioned SQLite storage and atomic change history. Support manual creation,
   editing, completion, dismissal, reopening, descriptions, due dates, optional
   priorities, and categories/projects without any journal records or AI service.
2. **Tasks workspace:** add a dedicated PySide6 Tasks tab with search, filtering,
   and upcoming, overdue, completed, and dismissed views using semantic theme tokens.
3. **Recurrence and local reminders:** add recurring occurrences, snoozing,
   reminder scheduling, and independent local delivery/retry tracking. Reminder
   delivery must never determine task status or silently reopen closed tasks.
4. **Daily overview:** build an in-app overview directly from SQLite, showing
   overdue, due-today, upcoming, and recently completed tasks. It must work offline
   with no journal entries or analysis engine.
5. **Optional morning email:** add explicitly enabled scheduled delivery of that
   overview, with content previews, privacy controls, and separate delivery state.
   Email failure must not alter tasks or disable offline functionality.
6. **Optional journal discovery:** suggest commitments, deadlines, new tasks,
   completions, and updates to existing tasks with immutable source provenance,
   duplicate checks, explicit confirmation, and manual-override precedence.

Phases 1–4 must be independently useful before journal discovery or email exists.
Email requires its own privacy/security review and explicit user configuration;
it is not a requirement for task management. This track may be scheduled separately
from journaling after scope approval; it has no transcription/model dependency.

See [the task-management plan](TASK_MANAGEMENT_PLAN.md) for scope, architecture,
phase acceptance criteria, and decisions still to resolve. Architecture principles
are recorded in [DECISIONS.md](DECISIONS.md).
