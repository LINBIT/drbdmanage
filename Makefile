U := $(shell ./setup.py versionup2date >/dev/null 2>&1; echo $$?;)

all: doc
	python setup.py build

doc:
	python setup.py build_man

install:
	python setup.py install

ifneq ($(U),0)
up2date:
		$(error "Update your Version stings/Changelogs")
else
up2date:
	$(info "Version strings/Changelogs up to date")
endif

release: up2date
	python setup.py sdist

deb: up2date
	debuild -i -us -uc -b

rpm: up2date doc
	python setup.py bdist_rpm

clean:
	python setup.py clean
	rm -f man-pages/*.gz
