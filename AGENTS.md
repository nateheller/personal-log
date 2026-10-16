# Development agent instructions

1. Read the current design specification and docs before changes.
2. Never commit or upload personal audio, transcripts, journals, keys, or real databases. Use synthetic fixtures.
3. Keep capture, storage, transcription, analysis, and UI/theme layers separate.
4. Recording cannot depend on transcription speed or model availability.
5. Preserve source transcripts and version refinements; never silently replace source text.
6. Use stable IDs, explicit schemas, deterministic migrations, and provenance.
7. Changes involving recording, encryption, retention, or restore require reproducible failure tests and independent review.
8. All analysis is optional, local by default, evidence-linked, and non-destructive.
9. Do not hardcode UI colors; use theme tokens.
10. Report tests actually run and remaining risks; never claim unverified PASS.
