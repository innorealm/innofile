.PHONY: clean fmt clippy test check build package

clean:
	cargo clean

fmt:
	cargo fmt

clippy:
	cargo clippy

test:
	cargo test

check: fmt clippy test

build:
	cargo build --release

package: clean check
	cargo package
