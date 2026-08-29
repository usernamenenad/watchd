# Security policy

## Project status

`watchd` is pre-alpha and has no supported production release. Security fixes currently target the `main` branch. Do not expose the current code to untrusted networks or rely on it for access-control decisions.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting from the repository Security tab. Do not open a public issue for suspected vulnerabilities or include credentials, connection strings, row contents, or exploit details in public logs.

Include:

- the affected commit or version;
- the security boundary that is crossed;
- reproduction steps or a minimal proof of concept;
- expected and observed behavior;
- any known mitigations.

The maintainer aims to acknowledge a report within seven days. Remediation and disclosure timelines depend on severity and project maturity and will be coordinated with the reporter.

## Security scope

High-priority areas include PostgreSQL credential exposure, unauthorized scope access, cross-tenant data delivery, unsafe snapshot or cursor handling, denial of service through unbounded resources, dependency or build-chain compromise, and secret leakage through logs or metrics.
