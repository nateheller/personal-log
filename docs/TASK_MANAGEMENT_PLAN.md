# Journal and independent task management

Status: product requirements and implementation plan only. No task-management
functionality, database migration, desktop change, or transcription change is
introduced by this document. Implementation requires a separately scoped increment.

Personal Log will support two independently usable functions: personal journaling
and personal task management. A person can use the complete task manager without
ever creating a journal entry, recording audio, running analysis, or enabling email.
Journal analysis provides an additional, optional route to discovering tasks.

## Product requirements

- Manually create, edit, complete, dismiss, and reopen tasks. Every task has a
  title and may have a description, due date, and optional priority.
- Task status is explicitly `pending`, `active`, `completed`, or `dismissed`.
  Completion and dismissal remain distinct, searchable outcomes. Reopening a
  completed task is an explicit user action; restoring a dismissed task is also
  explicit. Neither reminders nor analysis silently reopen either status.
- Support recurring tasks, categories/project organization, snoozing, and reminder
  scheduling. Snoozing changes reminder timing, not the underlying due date or task
  status unless the user separately edits those fields.
- Provide a dedicated Tasks tab in PySide6 with search/filtering and upcoming,
  overdue, completed, and dismissed views. Also expose due-today tasks in the daily
  overview. Include status, date, category/project, and priority filters, including
  tasks without a due date. Use the existing theme architecture and semantic tokens.
- Persist tasks and their change history in SQLite. Task operations must remain
  fully functional offline and without journal references or model availability.
- Journal-derived tasks retain source provenance. Task edits never change original
  journal text, source transcripts, or their existing version history.

## Boundaries and proposed records

This is a conceptual model, not a finalized SQL schema or migration specification.
Use stable IDs and explicit versions when implementation begins.

| Planned record | Responsibility |
| --- | --- |
| Task | Authoritative title, description, status, due date, optional priority, organization, and revision. No required journal foreign key. |
| TaskChange | Append-only record of creation, field/status changes, actor, timestamp, prior/resulting revision, and relevant confirmation or override. Commit with the task change atomically. |
| Category / Project | Organization independent of journal entries; final cardinality and category/project relationship remain a design decision. |
| Recurrence / Occurrence | Schedule and distinct task occurrences linked by stable identity. Creating a later occurrence must not reopen or erase a completed/dismissed occurrence. |
| Reminder / DeliveryAttempt | Scheduled and snoozed delivery, retry outcome, and delivery identity; separate from task status. |
| TaskSuggestion | Proposed new task or field/status update, review decision, candidate matches, and proposed target revision. Not a confirmed task. |
| TaskSourceLink | Optional evidence links to journal entry/session, exact transcript or journal revision, and passage identifiers; retain analysis provenance. A task may have zero or multiple links. |
| OverviewDelivery | Optional email schedule, content policy, overview identity, attempt history, and delivery result; not task state. |

The task service owns task validation, transitions, and transactions. The UI calls
that service; it does not independently infer or rewrite task state. Recording,
storage of journal audio, transcription, and analysis remain separate layers.
Task storage is separate in responsibility from journal storage; whether it shares
the application database is deferred to schema design. SQLite remains the source
of truth regardless of that packaging choice. No task operation waits on AI.

Local reminders and optional email consume committed task data. Delivery attempts,
failures, retries, and acknowledgment do not complete, dismiss, reactivate, or
otherwise mutate tasks. Re-evaluate reminder eligibility after task edits or
completion so outdated notifications are not delivered unnecessarily.

Future schema changes must use numbered deterministic migrations and preserve
history. No migration is part of this roadmap update. Privacy/security requirements
apply to task descriptions, audit history, derived evidence, and delivery content
as well as journal records; follow the existing staged security plan.

## Journal integration and user authority

Optional analysis may identify possible commitments, deadlines, and completion
statements. It may suggest a new task or an update to an existing task, including
proposed completion. Uncertain interpretations remain visibly uncertain. Preserve
the source entry/session, source version and passage, analysis version, and proposed
change so a person can inspect the evidence. Never invent a deadline from ambiguous
language; offer an unconfirmed interpretation for review instead.

Suggestions have their own review state and are clearly distinguished from
confirmed tasks. An accepted suggestion passes through the same task service and
audit path as manual work. Confirmation is required to create a task or apply an
analysis-derived field/status change. Rejecting a suggestion does not create or
dismiss a real task. A new manual task requires no analysis or source reference.

Before suggesting creation, compare existing tasks and prior suggestions. Stable
source/analysis identities must make repeated analysis idempotent; possible matches
are shown for confirmation rather than silently merged. Closed tasks participate
in duplicate checks. A matching completed or dismissed task is never automatically
reopened: offer a clearly reviewed choice to link, create a distinct task, or take
no action. Preserve explicit rejection and manual-override decisions across reruns.

Accepting an update must check that its target task revision still matches the
reviewed version. If the user has edited the task meanwhile, request fresh review
rather than overwriting the newer decision. Manual decisions always take precedence
over suggestions. Editing, completing, dismissing, or reopening a task never writes
back to the original journal text or source transcript. Unavailable source content
must not prevent independent task use or silently delete the task; preserve the
reference/history and show that evidence is unavailable.

## Daily overview and optional email

Generate the unified daily overview from a consistent SQLite view of confirmed
tasks. Include overdue, due-today, upcoming, and recently completed groups. Pending
and active tasks with elapsed due dates are overdue; completed and dismissed tasks
must not appear as outstanding work. Show dismissed tasks in their dedicated view.
Tasks without due dates remain accessible in the Tasks tab and search/filtering.
Define date boundaries, upcoming/recent windows, and user timezone explicitly;
use the same grouping rules in the application and email.

The in-app overview requires neither journal analysis nor network access. Optional
scheduled morning email delivers a task-database overview, not an AI-generated
interpretation of journal content. Email is off by default and requires explicit
enablement, recipient configuration, schedule/timezone selection, and a preview of
what will leave the device. A user must be able to disable delivery at any time.

Provide privacy controls for counts-only summaries, inclusion of task titles,
descriptions/dates, category/project exclusions, and individual task exclusions.
Default to minimal content, and omit journal quotations, transcripts, and audio.
Task titles/descriptions may themselves contain sensitive journal-derived content;
source linkage does not make them safe to email automatically. Treat email as an
explicit exception to local-only operation, with informed content selection.

Email scheduling/delivery runs separately from task operations. Use stable delivery
identities and bounded retry policies; avoid duplicate delivery where possible,
without promising exactly-once receipt from an external email provider. Report
failure locally, and keep task state unchanged. Offline use remains fully available;
late/skipped morning deliveries and retries require a documented policy. Provider,
credential protection, and content logging rules require review before implementation.

## Planned implementation phases and acceptance

1. **Standalone foundation.** Define contracts, status transitions, manual CRUD,
   completion/dismissal/reopening, organization, persistence, and atomic change
   history. Add future numbered migrations only in the implementation increment.
   Demonstrate the full manual lifecycle with an empty journal database, no AI,
   restart persistence, and rollback when history insertion fails.
2. **Tasks workspace.** Build the dedicated Tasks tab, task editing, search,
   filtering, and upcoming/overdue/completed/dismissed views. Validate keyboard use,
   date handling, theme tokens, and complete independence from journal entries.
3. **Recurrence and local reminders.** Define recurrence schedules and occurrence
   identity, schedule/snooze reminders, and track delivery separately. Test restart,
   retry, missed schedules, timezone/DST boundaries, recurrence edits, and closed
   occurrences. No duplicate occurrences or silent reopening; reminder failure
   leaves task status unchanged.
4. **Daily overview.** Share deterministic database grouping logic across the
   in-app overview and future delivery consumers. Test overdue/due-today/upcoming/
   recently completed boundaries, undated tasks, and offline operation with no
   journal data or analysis service.
5. **Optional morning email.** Add explicit opt-in, recipient and schedule settings,
   previews, content exclusions, credential protection, and delivery history/retries.
   Test disabled-by-default behavior, opt-out, offline/provider failures, duplicate
   attempts, privacy exclusions, and unchanged task state after all delivery outcomes.
   Require a privacy/security review before enabling external delivery.
6. **Optional journal discovery.** Add evidence-linked suggestions for new tasks,
   deadlines, completion, and updates. Test source fidelity, repeated-analysis
   deduplication, ambiguous deadlines, manual overrides, rejected suggestions,
   concurrent edits, and completed/dismissed matches. Require confirmation and
   verify that source journal/transcript records are unchanged. Disabling analysis
   must leave all task features operational.

Phases 1–4 deliver independently usable task management. Phase 5 is optional and
does not gate phase 6; journal discovery is optional and cannot gate the standalone
task workflow. Scheduling this track must not modify or interrupt ongoing
transcription development. Automated fixtures for future implementation must be
synthetic, with application data outside the repository.

## Decisions to resolve before implementation

- Date-only versus timed deadlines, timezone changes, and interpretation of
  upcoming/recent overview windows.
- Priority scale, category/project relationships, and completion/reopening effects
  on timestamps and reminder eligibility.
- Recurrence rules, missed occurrences, snooze semantics, and scope of recurrence
  edits (one occurrence versus future occurrences).
- Reminder behavior while the application is closed or Windows is asleep.
- Email provider/credential storage, recipient validation, retry/late-delivery
  policy, content privacy defaults, and interaction with future encrypted storage.
- Journal evidence availability and task-history behavior under future export,
  restore, and explicit source deletion; task operations must remain independent.

These are design questions, not permission to implement placeholders or change the
current recorder/transcription contracts. See [ROADMAP.md](ROADMAP.md),
[DECISIONS.md](DECISIONS.md), and [ANALYSIS_PROTOCOL.md](ANALYSIS_PROTOCOL.md).
