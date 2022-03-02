.PHONY: build-NodeJsLayer

build-NodeJsLayer:
	mkdir -p $(ARTIFACTS_DIR)/nodejs/
	cp package.json $(ARTIFACTS_DIR)/nodejs/
	cp package-lock.json $(ARTIFACTS_DIR)/nodejs/
	cd $(ARTIFACTS_DIR)/nodejs && npm install --production 
	cp -r ./pplibs/ $(ARTIFACTS_DIR)/nodejs/node_modules/
