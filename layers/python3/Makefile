.PHONY: build-Python3Layer

build-Python3Layer:
	mkdir -p "$(ARTIFACTS_DIR)/python/"
	# cp *.py "$(ARTIFACTS_DIR)/python"
	python -m pip install -r requirements.txt -t "$(ARTIFACTS_DIR)/python"
	cp -r ./pplibs/ $(ARTIFACTS_DIR)/python/
