GIT = git
INSTALLFILES=.installfiles
override GITHEAD := $(shell test -e .git && $(GIT) rev-parse HEAD)

U := $(shell ./setup.py versionup2date >/dev/null 2>&1; echo $$?;)

all: doc
	python setup.py build

doc:
	python setup.py build_man

install: drbdmanage/consts_githash.py
	python setup.py install --record $(INSTALLFILES)

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

release: up2date
	python setup.py sdist

debrelease: up2date
	echo 'recursive-include debian *' >> MANIFEST.in
	dh_clean
	make release
	git checkout MANIFEST.in

deb: up2date
	[ -d ./debian ] || (echo "Your checkout/tarball does not contain a debian directory" && false)
	debuild -i -us -uc -b

# it is up to you (or the buildenv) to provide a distri specific setup.cfg
rpm: up2date doc
	python setup.py bdist_rpm

.PHONY: drbdmanage/consts_githash.py
ifdef GITHEAD
override GITDIFF := $(shell $(GIT) diff --name-only HEAD 2>/dev/null |	\
			tr -s '\t\n' '  ' |		\
			sed -e 's/^/ /;s/ *$$//')
drbdmanage/consts_githash.py:
	@echo "DM_GITHASH = 'GIT-hash: $(GITHEAD)$(GITDIFF)'" > $@
else
drbdmanage/consts_githash.py:
	@echo >&2 "Need a git checkout to regenerate $@"; test -s $@
endif

clean:
	python setup.py clean
	rm -f man-pages/*.gz
