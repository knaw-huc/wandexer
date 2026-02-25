run:
	wandexer \
		--annorepo-host=https://preview.dev.diginfra.org/annorepo \
		--annorepo-container=israels \
		--config ./indexer/config.yml \
		--elastic-host=http://localhost:9200 \
		--elastic-index=isrent
#	--trace

.PHONY: install-dependencies
install-dependencies: | .installed
.installed:
	@echo "--- Checking global prerequisites  ---">&2
	@command -v python3 || (echo "Missing dependency: python3" && false)
	$(MAKE) env
	touch $@

.PHONY: env
env:
	@echo "--- Setting up virtual environment ---">&2
	python3 -m venv env \
		&& . env/bin/activate \
		&& pip install --upgrade pip \
		&& pip install build
	touch $@

.PHONY: clean-dependencies
clean-dependencies:
	@echo "--- Cleaning dependencies ---">&2
	-rm -rf env
	-rm -f .installed

.PHONY: dist
dist: | .dist
.dist:
	@echo "--- Building distribution files ---">&2
	. env/bin/activate && python -m build
	touch $@

.PHONY: clean
clean:
	-rm -rf dist
	-rm -f .dist

upload: dist
	@echo "--- Uploading distribution to pypi (requires auth) ---">&2
	twine upload dist/*
