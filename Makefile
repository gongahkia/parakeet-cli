.PHONY: build test lint release install

build:
	cargo build

test:
	cargo test --workspace

lint:
	cargo fmt --check
	cargo clippy -- -D warnings

release:
	cargo build --release

install: release
	cp target/release/parquet-lens /usr/local/bin/
