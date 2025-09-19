# Airflow DAG Impact Analysis GitHub Action

This GitHub Action analyzes changes to Airflow DAGs and provides impact analysis through DQLabs integration.

## Features

- **DAG Analysis**: Extracts DAG metadata including ID, schedule, tasks, and dependencies
- **Impact Analysis**: Integrates with DQLabs to analyze downstream impacts
- **Column Change Detection**: Tracks changes in SQL and YAML files
- **PR Comments**: Automatically posts analysis results as PR comments

## Setup

### 1. Required Secrets

Configure these secrets in your repository settings (Settings → Secrets and variables → Actions):

- `DQLABS_API_CLIENT_ID`: Your DQLabs API client ID
- `DQLABS_API_CLIENT_SECRET`: Your DQLabs API client secret
- `DQLABS_BASE_URL`: Base URL for DQLabs API
- `DQLABS_CREATELINK_URL`: URL for creating links in DQLabs

### 2. Workflow Configuration

The workflow is configured to trigger on pull requests that modify Python files:

```yaml
on:
  pull_request:
    paths:
      - '**/*.py'
```

## What It Analyzes

### Airflow DAGs (Python Files)
- DAG ID and metadata
- Task definitions and dependencies
- Schedule intervals
- Tags and descriptions
- File structure and imports

## Output

The action provides:
- Detailed analysis in PR comments
- Summary in GitHub Actions logs
- Impact analysis results from DQLabs

## Dependencies

- Node.js 18+
- Required npm packages (see `package.json`):
  - @actions/core
  - @actions/github
  - axios

## Usage

The action runs automatically on pull requests. No manual intervention required.

## Troubleshooting

1. **Missing Secrets**: Ensure all required secrets are configured
2. **API Errors**: Check DQLabs API credentials and URLs
3. **File Analysis Errors**: Verify file paths and permissions
