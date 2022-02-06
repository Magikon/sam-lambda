.PHONY: build-Python3Layer

build-Python3Layer:
	mkdir -p "$(ARTIFACTS_DIR)/python"
	# cp *.py "$(ARTIFACTS_DIR)/python"
	python3 -m pip install -r requirements.txt -t "$(ARTIFACTS_DIR)/python"
	cp -R $(PWD)/shared/python3/* $(ARTIFACTS_DIR)/python/
