default:
    @just --list

# Agent gegen das lokale Panel starten
dev:
    cargo run -p harrier -- agent run --panel ws://localhost:8080/agent/ws

test:
    cargo test --workspace

# TypeScript-Typen des Protokolls nach crates/protocol/bindings erzeugen
protocol:
    rm -rf crates/protocol/bindings
    cargo test -p harrier-protocol export_bindings --quiet

# Alles, was die CI prüft
check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    just protocol
    git diff --exit-code crates/protocol/bindings
