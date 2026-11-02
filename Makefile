PG_CONFIG ?= pg_config
FEATURES ?=
PGRX_PG_CONFIG_PATH := $(shell command -v $(PG_CONFIG))
export PGRX_PG_CONFIG_PATH
UNAME := $(shell uname -s)
EXT := $(if $(filter Darwin,$(UNAME)),dylib,so)
PKGLIBDIR := $(shell $(PG_CONFIG) --pkglibdir)
SHAREDIR := $(shell $(PG_CONFIG) --sharedir)

.PHONY: build test integration install
build:
	cargo build -p pg-cache --release --locked $(if $(FEATURES),--features $(FEATURES))
test:
	cargo test --locked
integration:
	cargo build -p pg-cache --locked
	python3 scripts/integration.py
install: build
	install -d "$(DESTDIR)$(PKGLIBDIR)" "$(DESTDIR)$(SHAREDIR)/extension"
	install -m 755 target/release/libpg_cache.$(EXT) "$(DESTDIR)$(PKGLIBDIR)/pg_cache.$(EXT)"
	install -m 644 crates/pg-cache/pg_cache.control crates/pg-cache/sql/pg_cache--0.1.0.sql "$(DESTDIR)$(SHAREDIR)/extension/"

ifneq (,$(findstring journal-prototype,$(FEATURES)))
	install -m 644 crates/pg-cache/pg_cache_journal.control "$(DESTDIR)$(SHAREDIR)/extension/"
	install -m 644 crates/pg-cache/sql/journal-prototype.sql "$(DESTDIR)$(SHAREDIR)/extension/pg_cache_journal--0.1.0.sql"
endif
