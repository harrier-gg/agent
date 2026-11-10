# Harrier Agent

Agent and CLI in one Rust binary (`harrier`). The agent runs on every machine that hosts game servers and connects to the panel **on its own** (outbound): no port forwarding and no certificate needed on the host.

| Crate | Purpose |
|---|---|
| `crates/protocol` | Messages agent ↔ panel. Source of truth; generates TypeScript types into `bindings/` |
| `crates/harrier` | Binary: `harrier agent run` (later also `install`, `update`, `doctor`, `template …`) |

## Development

Requirements: Rust (version from `rust-toolchain.toml`, installed automatically by rustup), [just](https://just.systems).

```bash
just dev        # agent against the local panel (ws://localhost:8080/agent/ws)
just protocol   # regenerate the TypeScript types, then run `just protocol` in ../backend
just check      # everything CI checks
```

Protocol versioning: see [ADR 0002](https://github.com/harrier-gg/.github/blob/main/adr/0002-agent-protocol-versioning.md). Log messages and CLI output are English only ([ADR 0003](https://github.com/harrier-gg/.github/blob/main/adr/0003-internationalization.md)).

See the [contributing guide](https://github.com/harrier-gg/.github/blob/main/CONTRIBUTING.md) for branches, PRs and releases.
