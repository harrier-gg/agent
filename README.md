# Harrier Agent

Agent and CLI in one Rust binary (`harrier`). The agent runs on every machine that hosts game servers and connects to the panel **on its own** (outbound): no port forwarding and no certificate needed on the host.

| Crate | Purpose |
|---|---|
| `crates/protocol` | Messages agent ↔ panel. Source of truth; generates TypeScript types into `bindings/` |
| `crates/harrier` | Binary: `harrier agent run` (later also `install`, `update`, `doctor`, `template …`) |

## Development

Requirements: Rust (version from `rust-toolchain.toml`, installed automatically by rustup), [just](https://just.systems).

The agent authenticates with its node's token ([ADR 0004](https://github.com/harrier-gg/.github/blob/main/adr/0004-agent-authentication.md)). Register the node in `../backend` with `just node-create <name>`, then:

```bash
export HARRIER_AGENT_TOKEN=hrr_…   # from the environment only, never as an argument
just dev        # agent against the local panel (ws://localhost:8080/agent/ws)
just protocol   # regenerate the TypeScript types, then run `just protocol` in ../backend
just check      # everything CI checks
```

The token never travels unencrypted over a network: the panel URL must use `wss://`, except for the own machine (`ws://localhost`). For a panel in a trusted home network without HTTPS, pass `--insecure` (or set `HARRIER_AGENT_INSECURE=true`); the agent warns on every start.

**Servers** run as Docker containers named `harrier-<serverId>` (Docker must be running). Each server keeps its data in its own directory under `~/.local/share/harrier/servers` (`--data-dir` / `HARRIER_DATA_DIR`); the container runs as the owner of that directory, without capabilities, with `no-new-privileges` and the memory limit from the panel. Containers keep running while the agent restarts; the agent re-attaches to them.

Exit code 2 means the panel rejected the agent for good (invalid token, unsupported protocol, or another agent took over the same token); restarting will not help.

Protocol versioning: see [ADR 0002](https://github.com/harrier-gg/.github/blob/main/adr/0002-agent-protocol-versioning.md). Log messages and CLI output are English only ([ADR 0003](https://github.com/harrier-gg/.github/blob/main/adr/0003-internationalization.md)).

See the [contributing guide](https://github.com/harrier-gg/.github/blob/main/CONTRIBUTING.md) for branches, PRs and releases.
