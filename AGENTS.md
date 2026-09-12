# AGENTS.md

Instructions, constraints, and safety guidelines for AI agents working on `crosslake`.

## Critical Security & Data Privacy Policy

This repository is public. **Under no circumstances should sensitive AWS, GCP, or personal infrastructure details be checked in or committed to git.**

### 1. Forbidden Information in Git
Never commit or log any of the following to git:
- **AWS Account IDs**: Any real 12-digit AWS account number (e.g., in S3 prefixes, policies, ARN strings).
- **ARNs (Amazon Resource Names)**: Any real IAM user, role, KMS key, S3 bucket, or CloudTrail ARN.
- **Credentials & Secrets**:
  - AWS Access Keys (`AKIA...`), Secret Access Keys, Session Tokens.
  - GCP Service Account JSON keys, HMAC keys, OAuth tokens.
  - `.env`, `*.pem`, `*.key`, `*config.yaml` (except `config.example.yaml`).
- **Internal Identifiers**: Real bucket names, internal IP addresses, organization IDs (`o-...`), or VPC IDs.

### 2. Required Redaction & Placeholder Conventions
Always use standard AWS/GCP documentation placeholders in code, configs, tests, and documentation:
- **Account ID**: `123456789012` or `<account-id>`
- **AWS Region**: `us-east-1` or `<region>`
- **Bucket Names**: `example-cloudtrail-bucket`, `crosslake-lake-<region>`
- **ARNs**:
  - `arn:aws:iam::123456789012:role/example-role`
  - `arn:aws:s3:::example-cloudtrail-bucket`
- **Keys**: `AKIAIOSFODNN7EXAMPLE`, `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
- **GCP Project**: `example-gcp-project` or `<gcp-project-id>`

### 3. Sanitizing CloudTrail Log Fixtures
CloudTrail records inherently contain sensitive data in `userIdentity` (real user ARNs, principal IDs, account IDs), `recipientAccountId`, `sourceIPAddress`, `requestParameters`, and `responseElements`.
- **Local fixtures**: Any sample JSON/Avro/Parquet files stored in the repository for unit testing must be synthetic or thoroughly anonymized/redacted before committing.
- **Live data**: Raw CloudTrail logs pulled from S3 belong exclusively in the local `data/` directory (which is strictly `.gitignore`d) and must never be staged or committed.

### 4. Agent Pre-Commit Checklist
Before proposing or executing any git commit:
1. Run `git diff --cached` or inspect changes.
2. Check for 12-digit numbers matching `\b[0-9]{12}\b`.
3. Check for `arn:aws:`.
4. Verify no real credential strings or config files are staged.
