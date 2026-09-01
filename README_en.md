# ChatHygiene

[中文](README.md) | English

## Introduction

ChatHygiene is a self-hosted Rust service. It uses a connected Telegram
Business bot to verify unknown private-message senders and, after explicit
enablement, delete high-confidence spam. One deployment serves one Telegram
account. Ordinary message bodies are not intentionally persisted, except for
samples explicitly labeled by the Owner.

Telegram delivers a message to the user before sending the update to the bot,
so a notification or chat-list entry can appear briefly before ChatHygiene
acts. Detection, rights, and Telegram-call failures preserve messages by
default.

## Features

- Unknown contacts receive an arithmetic challenge; an Owner reply makes the
  conversation `ACTIVE`.
- Deterministic local rules inspect text, captions, and Telegram entities
  without downloading attachments or calling an LLM.
- Only a score of `100` and verdict `SPAM` can lead to deletion;
  `SUSPICIOUS` is always retained.
- A first start defaults to dry-run and does not automatically read, delete,
  or soft-block messages.
- SQLite recovers recorded events, challenges, connection reconciliation, and
  outbox actions.
- The database manages the installation seed, Owner identity, bot-ID pin,
  candidates, and trusted connection.
- Every application start reconciles the Telegram webhook automatically; the
  operator does not generate application keys.

## How it works

```text
unknown message -> local detection -> safe message starts arithmetic verification
                                   -> high-confidence spam becomes a cleanup candidate
Owner reply -> ACTIVE; stop inspecting that conversation
```

The arithmetic challenge lasts two minutes and permits three numeric answers;
non-numeric messages do not consume an attempt. A correct answer moves the
conversation to `VERIFIED_WAITING_OWNER`. An Owner reply moves it to `ACTIVE`
until all observed Owner replies are deleted or the Owner runs
`/reset <chat_id>`.

Verdict boundaries:

| Score | Verdict | Behavior |
| --- | --- | --- |
| 0–49 | `ALLOW` | Keep the message and continue the lifecycle. |
| 50–99 | `SUSPICIOUS` | Keep the message for Owner review. |
| 100 | `SPAM` | Record evidence; clean up only when dry-run is off. |

## Deployment

### Prerequisites

- Linux, Docker Engine, and Docker Compose v2;
- a BotFather bot with Business/Secretary support enabled;
- a public HTTPS URL;
- a reverse proxy, tunnel, or gateway that terminates TLS and continuously
  forwards `/telegram/webhook`; and
- a protected deployment account, `.env`, and data directory.

The application always listens on plain HTTP `8080` inside the container.
Telegram accepts public webhook ports `443`, `80`, `88`, and `8443` only;
ChatHygiene still requires HTTPS on all four. Prefer public `443` TLS
termination forwarding to container `8080`. A URL such as
`https://host:8080/...` is rejected locally, and a Docker port mapping does not
provide TLS.

The default public URL is:

```dotenv
CHATHYGIENE_PUBLIC_WEBHOOK_URL=https://host/telegram/webhook
```

The reverse proxy must forward that public path to the same internal
`/telegram/webhook` route and preserve
`X-Telegram-Bot-Api-Secret-Token`. For example:

```nginx
location = /telegram/webhook {
    proxy_pass http://127.0.0.1:8080/telegram/webhook;
    proxy_set_header X-Telegram-Bot-Api-Secret-Token $http_x_telegram_bot_api_secret_token;
}
```

A different public path or query is valid only with a corresponding rewrite
to the fixed internal route. Telegram accepting the webhook declaration does
not prove that the proxy avoids `404`/`403` or preserves the secret header.

### Fresh deployment

A fresh installation needs only the bot token, public webhook URL, protected
data directory, and dry-run default. Create the bind source explicitly before
the first `docker compose up`. Do not let Compose auto-create the host
directory with its usual `0755` mode; the container entrypoint rejects it.

```bash
install -d -m 0700 data
cp .env.example .env
chmod 600 .env
stat -c '%a %n' data .env
```

If the deployment account differs from the current account, stop the service
and apply the required `chown` to these exact paths. Do not recursively change
an unknown directory. Expect `data` to be `700`, `.env` to be `600`, and both
to belong to the deployment account.

Fill `.env`:

```dotenv
CHATHYGIENE_BOT_TOKEN=token-issued-by-BotFather
CHATHYGIENE_PUBLIC_WEBHOOK_URL=https://host/telegram/webhook
CHATHYGIENE_DESTRUCTIVE_MODE=false
CHATHYGIENE_PORT=8080
RUST_LOG=info
```

`CHATHYGIENE_PORT` is only the host-to-fixed-container-`8080` mapping. It is
not an arbitrary Telegram public port.

Load the Release image for the VPS architecture and start it:

```bash
uname -m
docker load -i chathygiene-amd64-YYYY.MM.DD.tar
docker compose up -d
docker compose ps
curl --fail http://127.0.0.1:8080/health/live
curl --fail http://127.0.0.1:8080/health/ready
```

The stable new-bot order is:

1. Create and configure the bot in BotFather.
2. Deploy and start ChatHygiene with the bot token and public URL.
3. Wait for HTTP readiness and automatic webhook reconciliation.
4. Bind the bot once in Telegram Business with all four rights: reply, mark
   read, delete sent messages, and delete received/all messages.
5. Click Start in the ordinary private bot chat.
6. Read the prepared complete command:

   ```bash
   docker compose exec -T chathygiene cat /data/claim-code
   ```

   Use `cat data/claim-code` only when host ownership permits it.
7. Send the command in the ordinary private chat, then verify Owner,
   connection, and dry-run state.

`/claim` reads the Owner user ID from Telegram's authenticated
`message.from.id` and the delivery chat ID from `message.chat.id`.
`CHATHYGIENE_OWNER_USER_ID` is no longer a valid setting.

### Owner claim and `/start`

While unclaimed, ChatHygiene creates `claim-code` beside the SQLite database
with mode `0600`. It contains the complete `/claim <token>` command. An
unclaimed restart with the same database recreates the same command if the
file is missing. A successful claim removes only an exact safe copy whose
content, type, and ownership match. Unsafe, symlinked, wrong-owner, or
mismatched entries are never deleted automatically and require stopped
operator repair.

The file is not an application CLI and does not need a separate backup.
Deleting it is not revocation: a pre-claim database backup contains the master
seed that derives the same token. After a successful claim, delete the sent
`/claim` message from Telegram. Telegram history, notifications, terminal
output, and the clipboard are outside ChatHygiene's database/log redaction
boundary.

`/start` never sends the claim code and never manufactures a
`business_connection` update. Before claim it only points to the sibling file.
After claim it shows help only to the Owner; a non-Owner receives no response.

### Business connection cases

- New bot: bind once after the new service is ready.
- Old already-bound bot moved to a fresh VPS/database: normally disconnect and
  reconnect once after readiness; if `/health/ready` already reports
  `connection=candidate`, do not reconnect first.
- Restored claimed database with a trusted connection: do not reconnect.
- A claim that reports `connection=missing` remains valid; connect afterward.

Webhook lifecycle payloads are triggers only. `getBusinessConnection` is the
authority for enabled state, all four rights, Business user, and connection
generation. Business effects are blocked while a matching connection is still
reconciling. Keyless private/Owner setup messages remain deliverable.

Optional diagnostic:

```bash
set -a
source .env
set +a
curl --fail --silent --show-error \
  "https://api.telegram.org/bot${CHATHYGIENE_BOT_TOKEN}/getWebhookInfo"
```

`getWebhookInfo` can inspect Telegram's visible URL, pending count, and error,
but it cannot reveal or prove the current webhook secret.

### Startup and health state

Every start follows this one-way order:

1. database descriptor and orphan-claim path preflight;
2. migrations;
3. commit/load the master seed;
4. derive the bot-independent challenge key and Owner-claim token;
5. read-only validate existing claim target/temp with the claim token;
6. authenticated `getMe` outside every transaction;
7. pin or verify the positive bot ID;
8. derive the bot-bound webhook secret;
9. enter the global `PENDING` gate;
10. recover legacy connection facts;
11. import/load the immutable Owner;
12. perform current-state lookup for at most one trusted connection;
13. recover local non-connection events;
14. re-sign open legacy challenges to version 1;
15. reconcile `claim-code`;
16. bind internal `8080`;
17. automatically submit the full webhook declaration with
    `drop_pending_updates=false`;
18. launch notifier, processing, and outbox workers;
19. begin HTTP serving;
20. drain remaining recorded connection triggers; and
21. transition global state to `READY`.

`GET /health/live` is always `200` once HTTP is being served and is the
Docker/routing liveness signal. `GET /health/ready` is the state/alerting
signal. It returns `200` only when global state is `READY` and the
Owner/connection combination is legal, with exactly:

```json
{"status":"ok","owner":"claimed","connection":"enabled"}
```

`owner` is `claimed|unclaimed`. `connection` is
`missing|candidate|enabled|disabled|rights_incomplete|ambiguous`. Global
`PENDING`, `AUTH_FAILED`, trusted reconciliation `PENDING`, and corrupt or
contradictory combinations all return the same minimal `503`:

```json
{"status":"unavailable"}
```

The Telegram Owner `/health` command uses the same transactional classifier
and adds dry-run state. It is distinct from both HTTP probes.

A reverse proxy or orchestrator must not stop forwarding
`/telegram/webhook` because readiness is `503` or Docker temporarily reports
unhealthy. While the process is live, ingress must remain available: pending
reconciliation may need a new connection trigger. The ten-minute container
start period covers a worst-case 25 seconds for `getMe`, 25 seconds for at
most one trusted `getBusinessConnection`, and 430 seconds for webhook
reconciliation: 480 seconds total, plus 120 seconds for migrations, seed/pin,
challenge re-signing, claim-file work, listener/workers, and scheduling. It
does not promise to absorb an arbitrary historical backlog.

Changing `CHATHYGIENE_PUBLIC_WEBHOOK_URL` or the bot token requires a restart.
The next start reconciles Telegram before readiness.

### Bot identity and failure boundaries

The first authenticated `getMe` stores the positive bot ID. This is a
trust-on-first-use boundary: after the pin exists, a rotated token for the same
bot is mechanically accepted and another bot is rejected. A legacy database
has no bot ID before its first upgrade and therefore cannot detect that the
operator supplied another bot's valid token. Use the old bot's current token
for the first upgrade, record the returned bot ID and credential source or
attestation without recording the token, and never combine the first upgrade
with bot replacement.

A startup `getMe` `401/403` returns a redacted error before bot pin, global
state, or trusted connection changes, and HTTP is not served. A `401/403`
during startup trusted revalidation commits global `AUTH_FAILED` and aborts.
The same failure during served recovery first makes readiness the minimal
`503`, then exits through owned worker cleanup. Only recognized
connection-not-found is connection-specific. Timeouts, protocol errors, and
unknown rejection retain the trigger for retry instead of trusting stale
payload data.

### Database, backup, and permissions

The database contains the master seed, Owner, Business state, challenges, and
runtime settings. `.env` alone cannot restore them, and a SQLite backup alone
cannot run the service. Complete recovery needs the stopped
`chathygiene.db`, any `-wal`/`-shm`, and non-derived settings such as bot token,
public URL, and deployment/TLS configuration.

Permission contract:

- `data/`: deployment owner, `0700`;
- DB, WAL, SHM, `claim-code`, and database backups: `0600`;
- active `.env` and rollback `.env` backup: deployment owner, `0600`; and
- rollback environment backup outside the Git checkout and Docker build
  context, for example `/var/backups/chathygiene/pre-upgrade.env`.

The container uses `umask 077` and fails closed on the default `/data` or an
existing DB/sidecar with unsafe mode or type. When the image runs as root, new
bind-mounted files may be root-owned. The container command is the safe claim
read path; not every host user is promised direct access. The database seed
derives the claim code, so protecting only the sibling file is insufficient.

Restoring a `CLAIMED` database restores the same Owner with no claim step and
removes only an exact safe stale claim file. Restoring an `UNCLAIMED` or
pre-claim backup recreates the same code and can promote a retained matching
candidate. Never run any two databases against the same Telegram bot at once.
They compete for the bot's single webhook, and cloned databases also share
derived credentials.

### One-time upgrade

1. Run `/dry_run on` on the old instance and confirm `/health`.
2. Stop the old instance. Never chmod or copy a live SQLite/WAL set.
3. Back up the exact database paths that exist, and copy old `code/.env` to a
   protected location outside the checkout/build context:

   ```bash
   docker compose stop chathygiene
   sudo install -d -m 0700 /var/backups/chathygiene
   sudo install -m 0600 .env /var/backups/chathygiene/pre-upgrade.env
   cp -p data/chathygiene.db /var/backups/chathygiene/
   test ! -e data/chathygiene.db-wal || cp -p data/chathygiene.db-wal /var/backups/chathygiene/
   test ! -e data/chathygiene.db-shm || cp -p data/chathygiene.db-shm /var/backups/chathygiene/
   checkout_path="$(realpath .)"
   rollback_path="$(realpath /var/backups/chathygiene/pre-upgrade.env)"
   case "$rollback_path" in "$checkout_path"|"$checkout_path"/*) exit 1 ;; esac
   ```

4. With an editor, remove `CHATHYGIENE_WEBHOOK_SECRET`,
   `CHATHYGIENE_CHALLENGE_HMAC_KEY`, and `CHATHYGIENE_OWNER_USER_ID` from the
   active `.env`, and add the public URL. Retain old values only in the
   external rollback backup. Never print environment values.
5. While stopped, apply exact `chmod 600` to each DB/WAL/SHM that exists, each
   database backup, active `.env`, and external rollback `.env`; apply
   `chmod 700` to `data`. If needed, use
   `sudo chown deployment:deployment <exact-path>`. Do not use a recursive
   glob.

   ```bash
   chmod 700 data
   chmod 600 .env data/chathygiene.db
   test ! -e data/chathygiene.db-wal || chmod 600 data/chathygiene.db-wal
   test ! -e data/chathygiene.db-shm || chmod 600 data/chathygiene.db-shm
   sudo chmod 600 /var/backups/chathygiene/pre-upgrade.env
   sudo chmod 600 /var/backups/chathygiene/chathygiene.db
   ```
6. Start the new image with the same bot's current token. Do not change bots
   during the first pin.
7. Inspect `/health/live`, `/health/ready`, Owner `/health`, optional
   `getWebhookInfo`, and dry-run. Claim only if the old database had no trusted
   connection.

After recorded-event recovery, exactly one old Business connection is
imported as Owner. Zero produces `UNCLAIMED`; multiple fail closed. Open
challenges are re-signed automatically and correct answers remain valid. The
new process naturally ignores the three old variables, but the active Compose
environment must still omit them. Acceptance checks variable-name presence
only and never prints values.

### Rollback

Rollback requires the old image, the complete pre-upgrade database set, and
the externally saved old `.env` together:

```bash
docker compose stop chathygiene
install -m 0600 /var/backups/chathygiene/pre-upgrade.env .env
# Restore pre-upgrade chathygiene.db, -wal, -shm and the old image before start.
set -a
source .env
set +a
curl --fail --silent --show-error --request POST \
  "https://api.telegram.org/bot${CHATHYGIENE_BOT_TOKEN}/setWebhook" \
  --data-urlencode "url=https://host/telegram/webhook" \
  --data-urlencode "secret_token=${CHATHYGIENE_WEBHOOK_SECRET}" \
  --data-urlencode "drop_pending_updates=false"
```

That manual webhook command exists only for old-release rollback
compatibility. Restoring the backup discards post-upgrade state. Editing
`_sqlx_migrations` or deleting `key_material` is not rollback or rotation.

Version 1 has no supported in-place master-seed rotation and no Owner
reset/rebind command. Deleting or changing either singleton is corruption, not
rotation. A future rebind design must ensure that an old token retained in
Telegram history or a pre-claim backup can never become valid again.

## Usage

Owner commands must come from the ordinary private bot chat:

| Command | Purpose |
| --- | --- |
| `/start` | Before claim, points to `claim-code`; after claim, shows Owner help. |
| `/health` | Shows the shared Owner/connection classification and `dry_run=on|off`. |
| `/dry_run on` | Increases safety independently and persists the setting. |
| `/dry_run off` | Enables real actions only with one enabled trusted connection and all four rights. |
| `/errors [1..20]` | Shows redacted errors. |
| `/inspect <chat_id>` | Shows known conversation state. |
| `/reset <chat_id>` | Deletes bot-known messages and resets the lifecycle. |
| `/unblock <chat_id>` | Clears a local soft block. |
| `/mark_spam` / `/mark_ham` | Labels replied text/caption samples. |

Use a non-contact account to test ordinary and high-confidence spam messages
while still in dry-run. Run `/dry_run off` only after readiness, Owner
`/health`, all four Business rights, traces, and `/errors` are clean.

## Configuration

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `CHATHYGIENE_BOT_TOKEN` | yes | — | BotFather token; always redacted. |
| `CHATHYGIENE_PUBLIC_WEBHOOK_URL` | yes | — | HTTPS public URL on `443/80/88/8443`. |
| `CHATHYGIENE_DATABASE_URL` | no | `sqlite://data/chathygiene.db` | Compose fixes this to `/data/chathygiene.db`. |
| `CHATHYGIENE_DESTRUCTIVE_MODE` | no | `false` | Startup default only when no persisted runtime setting exists. |
| `CHATHYGIENE_PORT` | Compose only | `8080` | Host mapping; application/container target stays `8080`. |
| `RUST_LOG` | no | `info` | Log filter; sensitive values remain excluded. |

The application does not read old manual-secret or numeric-Owner settings.
Ordinary message bodies, captions, filenames, usernames, display names, bot
token, master seed, derived keys, claim token, complete claim command, and HTTP
request bodies do not enter application logs. Explicit `/mark_spam` and
`/mark_ham` samples are the exception.

## Development and testing

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
docker build -t chathygiene:local .
```

The repository contains Rust source, SQLx migrations, Docker/Compose inputs,
and integration tests. The service retains one SQLite pool connection, one
processing worker, capacities `32/128`, and a `250ms` outbox poll.

## Troubleshooting

| Symptom | Cause or action |
| --- | --- |
| Missing/corrupt `key_material`, unknown key version, or checksum mismatch | Fail closed; restore a complete backup and never delete the singleton. |
| Pinned bot mismatch | Use the original bot's token for this database; never edit the pin. |
| Startup `getMe` authentication failure | Redacted exit without serving; repair the token and restart. |
| `AUTH_FAILED` or long pending trusted/trigger reconciliation | Repair bot rights/network and preserve ingress; triggers retry. |
| Invalid Owner singleton or more than one legacy trusted connection | Fail closed; restore a consistent backup instead of selecting a row manually. |
| Orphan claim file beside a missing DB, or claim file from another DB | Stop and verify file/database provenance; do not let the app overwrite it. |
| Claim target/temp is a symlink, wrong owner/mode, or mismatched content | Repair or move the exact entry while stopped; the app will not delete it. |
| Broad `data`, DB sidecar, active `.env`, or rollback `.env` permissions | Stop and correct the exact paths to `0700/0600`. |
| Malformed legacy challenge expression or unsupported HMAC version | Startup fails; restore an uncorrupted backup. |
| Unsupported public port or non-HTTPS URL | Use `443/80/88/8443` with TLS termination. |
| Proxy returns `404/403` | Check public-path rewrite and secret-header preservation. |
| Automatic webhook permanent rejection or exhausted transient retries | Inspect the redacted class/status, repair URL/bot/network, and restart. |
| Candidate missing or expired | Reconnect once to create a fresh trigger. |
| `connection=ambiguous` | No row is auto-selected; let old candidates expire, then make one fresh same-Owner connection. |
| Readiness `503` while live is `200` | Keep forwarding the webhook and repair the reported Owner/connection state. |
| `/dry_run off` is rejected | Restore one enabled trusted connection with all four rights first. |

## License

This project is licensed under the [MIT License](LICENSE).
