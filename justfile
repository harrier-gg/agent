default:
    @just --list

# Run the agent against the local panel
dev:
    cargo run -p harrier -- agent run --panel ws://localhost:8080/agent/ws

test:
    cargo test --workspace

# Generate the TypeScript types of the protocol into crates/protocol/bindings
protocol:
    rm -rf crates/protocol/bindings
    cargo test -p harrier-protocol export_bindings --quiet

# Everything CI checks
check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    just protocol
    git diff --exit-code crates/protocol/bindings
