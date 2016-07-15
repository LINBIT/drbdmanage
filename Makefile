GIT = git
INSTALLFILES=.installfiles
PYTHON = python2
override GITHEAD := $(shell test -e .git && $(GIT) rev-parse HEAD)

U := $(shell $(PYTHON) ./setup.py versionup2date >/dev/null 2>&1; echo $$?;)
TESTS = $(wildcard unit-tests/*_test.py)

all: doc
	$(PYTHON) setup.py build

doc:
	$(PYTHON) setup.py build_man

install: drbdmanage/consts_githash.py
	$(PYTHON) setup.py install --record $(INSTALLFILES)

uninstall:
	test -f $(INSTALLFILES) && cat $(INSTALLFILES) | xargs rm -rf || true
	rm -f $(INSTALLFILES)

ifneq ($(U),0)
up2date:
	$(error "Update your Version strings/Changelogs")
else
up2date: drbdmanage/consts_githash.py
	$(info "Version strings/Changelogs up to date")
endif

release: up2date clean
	$(PYTHON) setup.py sdist
	@echo && echo "Did you run distclean?"

debrelease: up2date clean
	echo 'recursive-include debian *' >> MANIFEST.in
	dh_clean
	make release
	git checkout MANIFEST.in

deb: up2date
	[ -d ./debian ] || (echo "Your checkout/tarball does not contain a debian directory" && false)
	debuild -i -us -uc -b

# it is up to you (or the buildenv) to provide a distri specific setup.cfg
rpm: up2date doc
	$(PYTHON) setup.py bdist_rpm

.PHONY: drbdmanage/consts_githash.py
ifdef GITHEAD
override GITDIFF := $(shell $(GIT) diff --name-only HEAD 2>/dev/null | \
			grep -vxF "MANIFEST.in" | \
			tr -s '\t\n' '  ' | \
			sed -e 's/^/ /;s/ *$$//')
drbdmanage/consts_githash.py:
	@echo "DM_GITHASH = 'GIT-hash: $(GITHEAD)$(GITDIFF)'" > $@
else
drbdmanage/consts_githash.py:
	@echo >&2 "Need a git checkout to regenerate $@"; test -s $@
endif

md5sums:
	CURDATE=$$(date +%s); for i in $$(${GIT} ls-files | sort); do md5sum $$i >> md5sums.$${CURDATE}; done

clean:
	$(PYTHON) setup.py clean
	rm -f man-pages/*.gz

distclean: clean
	git clean -d -f || true

check:
	$(PYTHON) $(TESTS)
