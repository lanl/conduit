NAME := conduit-slurm-plugin
SPEC := $(NAME).spec
VERSION := $(shell awk '/^Version:/ {print $$2}' $(SPEC))
RELEASE := $(shell awk '/^Release:/ {print $$2}' $(SPEC) | sed 's/%{?dist}//')
TOPDIR := $(NAME)-$(VERSION)
TARBALL := dist/$(TOPDIR).tar.gz

RPMTOP := $(HOME)/rpmbuild
SOURCES := $(RPMTOP)/SOURCES
SPECS := $(RPMTOP)/SPECS

FILES := burst_buffer.lua burst_buffer.conf $(SPEC)
BIN := conduit

.PHONY: help check clean dist tarball prep rpm srpm

help:
	@echo "Targets:"
	@echo "  make check   - verify conduit and required files exist"
	@echo "  make rpm     - build binary RPM (and SRPM) into ~/rpmbuild/"
	@echo "  make srpm    - build only SRPM"
	@echo "  make clean   - remove dist/ and build artifacts"

check:
	@test -f $(SPEC) || (echo "ERROR: missing $(SPEC)"; exit 1)
	@for f in $(FILES); do test -f $$f || (echo "ERROR: missing $$f"; exit 1); done
	@test -f $(BIN) || (echo "ERROR: missing ./$(BIN). Build it and place it in repo root."; exit 1)
	@test -x $(BIN) || (echo "ERROR: ./$(BIN) is not executable (chmod +x $(BIN))"; exit 1)
	@echo "OK: inputs present (version=$(VERSION))"

dist:
	@mkdir -p dist/$(TOPDIR)

tarball: check dist
	@rm -rf dist/$(TOPDIR)/*
	@cp -a $(FILES) $(BIN) dist/$(TOPDIR)/
	@if [ -f LICENSE ]; then cp -a LICENSE dist/$(TOPDIR)/; fi
	@if [ -f ../../docs/integrations/slurm-plugin.md ]; then cp -a ../../docs/integrations/slurm-plugin.md dist/$(TOPDIR)/; fi
	@tar -C dist -czf $(TARBALL) $(TOPDIR)
	@echo "Wrote $(TARBALL)"

prep: tarball
	@mkdir -p $(SOURCES) $(SPECS)
	@cp -a $(TARBALL) $(SOURCES)/$(NAME)-$(VERSION).tar.gz
	@cp -a $(SPEC) $(SPECS)/
	@echo "Staged:"
	@echo "  $(SOURCES)/$(NAME)-$(VERSION).tar.gz"
	@echo "  $(SPECS)/$(SPEC)"

rpm: prep
	@rpmbuild -ba $(SPECS)/$(SPEC)
	@echo
	@echo "Built RPMs:"
	@ls -1 $(RPMTOP)/RPMS/*/$(NAME)-$(VERSION)-*.rpm 2>/dev/null || true
	@echo "Built SRPMs:"
	@ls -1 $(RPMTOP)/SRPMS/$(NAME)-$(VERSION)-*.src.rpm 2>/dev/null || true

srpm: prep
	@rpmbuild -bs $(SPECS)/$(SPEC)
	@echo
	@echo "Built SRPMs:"
	@ls -1 $(RPMTOP)/SRPMS/$(NAME)-$(VERSION)-*.src.rpm 2>/dev/null || true

clean:
	@rm -rf dist
