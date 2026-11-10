# Harrier Agent

Agent und CLI in einer Rust-Binary (`harrier`). Der Agent läuft auf jedem Rechner, der Spieleserver hostet, und verbindet sich **von sich aus** mit dem Panel – keine Portfreigabe, kein Zertifikat auf dem Host nötig.

| Crate | Zweck |
|---|---|
| `crates/protocol` | Nachrichten Agent ↔ Panel. Quelle der Wahrheit, erzeugt TypeScript-Typen nach `bindings/` |
| `crates/harrier` | Binary: `harrier agent run` (später auch `install`, `update`, `doctor`, `template …`) |

## Entwickeln

Voraussetzungen: Rust (Version aus `rust-toolchain.toml`, wird von rustup automatisch installiert), [just](https://just.systems).

```bash
just dev        # Agent gegen das lokale Panel (ws://localhost:8080/agent/ws)
just protocol   # TypeScript-Typen neu erzeugen, danach in ../backend `just protocol`
just check      # Alles, was die CI prüft
```

Protokoll-Versionierung: siehe ADR 0002 im `.github`-Repo.
