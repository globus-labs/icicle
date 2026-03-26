.PHONY: deps compile clean

PYTHON ?= python3.11
MY_DEPS = my_deps

deps: $(MY_DEPS)/.stamp

$(MY_DEPS)/.stamp:
	mkdir -p $(MY_DEPS)
	$(PYTHON) -m pip install --target $(MY_DEPS)/ \
		--platform manylinux2014_x86_64 \
		--only-binary=:all: \
		ddsketch datasketches numpy==1.24.4 globus_sdk \
		--upgrade
	touch $@

compile: deps
	bash compile.sh

clean:
	rm -rf target $(MY_DEPS) my_deps.zip utils.zip resources.zip
