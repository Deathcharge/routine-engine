# Security Policy

## Supported versions

Security fixes are currently made on the latest `0.1.x` release candidate and the default branch. The archived top-level `routine_engine/` extraction is not a supported public API and is excluded from distributions.

## Report a vulnerability

Email [support@samsarix.com](mailto:support@samsarix.com) with:

- the affected version or commit;
- a minimal reproduction or source trace;
- likely impact and prerequisites;
- any suggested remediation;
- your preferred disclosure credit.

Please do not open a public issue for an undisclosed vulnerability. Samsarix LLC will acknowledge a report as soon as practical, coordinate validation and remediation, and agree on disclosure timing with the reporter. Do not access data you do not own or disrupt services while testing.

## Security model

Workflow definitions are untrusted data; registered actions are trusted application code. Routine Engine validates graph and resource bounds and never evaluates definition-supplied code, commands, imports, SQL, paths, or URLs. It is not a sandbox. Applications are responsible for authorization and validation inside every action they register.

The JSON store is intended for a trusted local filesystem. It provides atomic replacement and in-process locking, not encryption, access control, tamper evidence, or multi-process transactions. Do not persist secrets or sensitive action outputs unless the surrounding filesystem and application controls are appropriate.
