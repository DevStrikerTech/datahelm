# Deployment Workflow

The repository ships with `.github/workflows/deploy-release.yml`.

## Triggers

- Automatic production deployment after successful `Docker Release` on `master`.
- Manual deployment via **Actions -> Deploy Release -> Run workflow** for:
  - `staging`
  - `production`

## Required GitHub Secrets

### Shared registry secrets

- `GHCR_USERNAME`: account/user with read access to GHCR package
- `GHCR_READ_TOKEN`: PAT with at least `read:packages`

### Staging host secrets

- `STAGING_SSH_HOST`
- `STAGING_SSH_PORT` (optional; defaults to `22`)
- `STAGING_SSH_USER`
- `STAGING_SSH_KEY`
- `STAGING_APP_ENV_FILE` (absolute path to env file on host)

### Production host secrets

- `PROD_SSH_HOST`
- `PROD_SSH_PORT` (optional; defaults to `22`)
- `PROD_SSH_USER`
- `PROD_SSH_KEY`
- `PROD_APP_ENV_FILE` (absolute path to env file on host)

## Runtime behavior

On the remote host, the workflow:

1. Logs in to GHCR
2. Pulls selected image tag
3. Replaces existing container
4. Starts container on port `3000`
