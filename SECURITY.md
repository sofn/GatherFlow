# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

Please report security vulnerabilities privately via [GitHub Security Advisories](https://github.com/sofn/GatherFlow/security/advisories/new).

We will acknowledge receipt within 7 business days and aim to provide a fix or assessment within 30 days. Please do not open public issues for security vulnerabilities.

## Disclosure Policy

This project follows coordinated disclosure. Once a fix is released, we will publish a security advisory and credit the reporter unless they request otherwise.

## Security Hardening

- Dependencies are pinned and verified with Gradle dependency verification (SHA-256 + PGP signatures).
- CI runs static application security testing (SAST) on every push and pull request.
