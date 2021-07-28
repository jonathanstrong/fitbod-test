export MAKEFLAGS        := "-j8"

cargo +args='':
    cargo {{args}}

check +args='':
    @just cargo check {{args}}

debug-build binary_name +args='':
    @just cargo build --bin {{binary_name}} {{args}}

release-build binary_name +args='':
    @just cargo build --bin {{binary_name}} --release {{args}}

example name +args='':
    @just cargo build --example {{name}} --features examples {{args}}

test +args='':
    @just cargo test {{args}}

# cargo doc --open
doc +args='':
    @just cargo doc --open {{args}}

# just rebuild docs, don't open browser page again
redoc +args='': 
    @just cargo doc {{args}}

# like doc, but include private items
doc-priv +args='':
    @just cargo doc --open --document-private-items {{args}}

bench +args='':
    @just cargo bench {{args}}

update +args='':
    @just cargo update {{args}}

# blow away build dir and start all over again
rebuild:
    just cargo clean
    just update
    just test
