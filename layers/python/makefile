.PHONY: build-PythonLayer

build-PythonLayer:
	mkdir -p "$(ARTIFACTS_DIR)/python"
	# cp *.py "$(ARTIFACTS_DIR)/python"
	python -m pip install -r requirements.txt -t "$(ARTIFACTS_DIR)/python"
	cp -R $(PWD)/shared/python/* $(ARTIFACTS_DIR)/python/
