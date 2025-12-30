# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue, please report it responsibly.

### How to Report

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please report security issues via:

1. **GitHub Security Advisories** (preferred): [Report a vulnerability](https://github.com/conduktor/streamt/security/advisories/new)
2. **Email**: security@conduktor.io

### What to Include

When reporting, please provide:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response Expectations

This is a community-driven open source project maintained on a best-effort basis. We do not guarantee specific response times, but we will:

- Acknowledge reports as soon as we can
- Prioritize critical vulnerabilities
- Keep you informed of progress when possible
- Credit reporters in release notes (unless you prefer anonymity)

Community members are welcome to submit fixes via pull request for faster resolution.

## Security Best Practices

When using streamt:

- Keep streamt updated to the latest version
- Store credentials securely (use environment variables, not config files)
- Review generated configurations before deployment
- Use network isolation for Kafka/Flink infrastructure
- Follow the principle of least privilege for service accounts

## Scope

This security policy applies to:

- The streamt CLI tool
- Official streamt documentation
- streamt GitHub repository

Third-party integrations (Kafka, Flink, Connect) have their own security policies.
