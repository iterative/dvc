# Run sharness tests
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#
# NOTE: run with TEST_VERBOSE=1 for verbose sharness tests.

T = $(sort $(wildcard t[0-9][0-9][0-9][0-9]-*.t))
LIBDIR = lib
SHARNESSDIR = sharness
AGGREGATE = $(LIBDIR)/$(SHARNESSDIR)/aggregate-results.sh


# Add below the binaries that this project generates or needs.
# For example:
# BINS = bin/ipfs
BINS =

all: aggregate

clean: clean-test-results
	@echo "*** $@ ***"
	# Clean binaries below.
	# For example:
	# -rm -rf bin/ipfs

clean-test-results:
	@echo "*** $@ ***"
	-rm -rf test-results

$(T): clean-test-results deps
	@echo "*** $@ ***"
	./$@

aggregate: clean-test-results $(T)
	@echo "*** $@ ***"
	ls test-results/t*-*.*.counts | $(AGGREGATE)

# Add below some needed dependencies.
# For example:
# deps: sharness $(BINS) curl
deps: sharness dvc $(BINS)

sharness:
	@echo "*** checking $@ ***"
	lib/install-sharness.sh

dvc:
	@echo "*** installing $@ ***"
	./install-dvc.sh

# Add below other targets like:
# - the targets needed to build binaries,
# - targets using special compile flags,
# - targets to check or install dependencies.
#
# For example:
#
# GOFLAGS =
#
# bin/%: FORCE
# 	cd .. && make GOFLAGS=$(GOFLAGS) $@
#
# race:
# 	make GOFLAGS=-race all
#
# curl:
#	@which curl >/dev/null || (echo "Please install curl!" && false)

.PHONY: all clean clean-test-results $(T) aggregate deps sharness FORCE

