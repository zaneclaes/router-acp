# Multiple provider accounts

`router-acp` can keep more than one login for the same ACP adapter. Goose CLI,
Goose desktop and other ACP clients get the same capability by launching the
router over stdio.

An `agents[].accounts` entry expands the base agent into candidates named
`agent@account/model`. The base agent's command, model list, model selector,
auth probe, usage source, and lineage are inherited by every account. Account
environment entries are applied after `command.env`, so an account entry
overrides a same-named base variable. The account's environment is also used
for auth probes, provider token and usage reads, usage snapshots, and Codex
app-server or rollout reads. The router process itself is not changed.

## Two Claude accounts

Keep the existing Claude login as `existing`: its empty `env` list means that
Claude inherits the router's environment. The example assumes your existing
login uses the normal default configuration directory, `~/.claude`, with no
global provider-auth overrides. If your existing login already uses a custom
`CLAUDE_CONFIG_DIR`, set that path explicitly on `existing`. Put the second
login in a separate directory and use per-command assignments instead of
exporting `CLAUDE_CONFIG_DIR` in the shell profile.

For directory-based OAuth accounts, launch the router without global
`CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_AUTH_TOKEN` or `ANTHROPIC_API_KEY` overrides:
the adapter inherits them and may use that credential instead of the login
in the account's directory. `command.env` is also inherited by every account.

Create the private directory and start the second login with a per-command
environment assignment:

```sh
umask 077
mkdir -p "$HOME/.config/router-acp/accounts/claude/personal"
CLAUDE_CONFIG_DIR="$HOME/.config/router-acp/accounts/claude/personal" claude auth login
```

When the browser opens, choose the identity for the second account. The
normal Claude login flow stores and refreshes that account in its own store,
preserving the existing login. On macOS Claude uses a Keychain service scoped
to that directory; the router reads the same service. Check both logins with:

```sh
claude auth status
CLAUDE_CONFIG_DIR="$HOME/.config/router-acp/accounts/claude/personal" claude auth status
```

To re-authenticate:

```sh
# Existing account: uses the default Claude directory.
claude auth login

# Personal account: affects only the isolated directory.
CLAUDE_CONFIG_DIR="$HOME/.config/router-acp/accounts/claude/personal" claude auth login
```

Use the configuration in [examples/router-accounts.yaml](../examples/router-accounts.yaml)
as your router configuration, with `claude-agent-acp` installed on `PATH`:

```sh
router-acp check-config --config examples/router-accounts.yaml
router-acp serve --config examples/router-accounts.yaml
```

Configure your ACP client to launch that `serve` command; see [GOOSE.md](../GOOSE.md)
for Goose's shim setup. The example creates these candidates:

```text
claude@existing/opus
claude@existing/sonnet
claude@personal/opus
claude@personal/sonnet
```

The explicit switch form includes the account name, for example:

```text
[router: switch=claude@personal/opus]
```

Automatic routing and failover can choose either account's candidates. Hot
failover carries partial text and tool status through the failure path to the
replacement account. Cancellation never triggers failover.

## Reserves and cordons

`reserve_capacity` is per account. Each value is a percentage from 0 through
100; positive values require both `cordon.enabled: true` and a
`usage_source`. In the example, `weekly: 10` and `session: 20` preserve the
last 10% of the weekly window and the last 20% of the session window for that
account. The router hard-cordons a candidate when usage reaches or exceeds
the corresponding `100 - reserve` threshold, even if paid overage remains. Model
scopes and provider reset times still apply.

An account without a `reserve_capacity` block inherits the base agent's
reserves. An account that supplies the block replaces it; omitted window
values in that block default to zero.

Enforcement uses the most recent provider reading; polling and the shared
cache control when a new reading becomes available. A confirmed reserve
breach with no reported reset gets a temporary 15-minute cordon, replaced by
the next reading. Failed or unknown usage reads cannot measure a reserve.

When a candidate is cordoned, automatic routing and failover skip it. There is
no all-cordoned fallback: if both Claude accounts are cordoned, wait for a
reset or change the configuration. A positive reserve on one account does not
consume the other account's reserve.

After changing the YAML, restart the router and its Goose client connection so
the expanded account candidates and environment take effect. Remove an
account entry and restart first when retiring an account; only after the
router has stopped using it should you revoke its login:

```sh
CLAUDE_CONFIG_DIR="$HOME/.config/router-acp/accounts/claude/personal" claude auth logout
```

Existing exact candidate references such as `claude/opus` become
`claude@existing/opus`. Update configured routing pools, evaluators and pins
when enabling accounts; `claude@*/*` matches every account for that adapter.

## Adding a third account

Add another uniquely named account under the same `claude` agent, with its own
private directory and its own reserve values:

```yaml
      - name: work
        env:
          - name: CLAUDE_CONFIG_DIR
            value: ${HOME}/.config/router-acp/accounts/claude/work
        reserve_capacity:
          weekly: 10
          session: 20
```

Create and authenticate it with the same per-command pattern, replacing
`personal` with `work`. Account names may not contain `/` or `@`.

## Codex analogue

The same isolation pattern applies to Codex with `CODEX_HOME`: configure a
separate account entry whose `env` sets `CODEX_HOME` to
`${HOME}/.config/router-acp/accounts/codex/personal`, and log in with:

```sh
CODEX_HOME="$HOME/.config/router-acp/accounts/codex/personal" codex login
```

Keep `CODEX_HOME` out of the global shell environment. The router uses that
same directory for Codex authentication, usage snapshots, and its
`codex app-server` rate-limit query.
