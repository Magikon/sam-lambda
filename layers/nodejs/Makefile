.PHONY: build-NodeJsLayer

build-NodeJsLayer:
	mkdir -p $(ARTIFACTS_DIR)/nodejs/
	cp package.json $(ARTIFACTS_DIR)/nodejs/
	cp package-lock.json $(ARTIFACTS_DIR)/nodejs/
	cd $(ARTIFACTS_DIR)/nodejs && npm install --production --arch=x64 --platform=linux
	cp -R $(PWD)/shared/nodejs/* $(ARTIFACTS_DIR)/nodejs/node_modules/
