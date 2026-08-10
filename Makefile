.PHONY: build test publish release clean help

GRADLEW := ./gradlew

help:
	@echo "Targets:"
	@echo "  build    - Compile, test, and assemble the project"
	@echo "  test     - Run unit tests"
	@echo "  dist     - Build the GitHub distribution zip (distZip)"
	@echo "  publish  - Publish artifacts to Maven Central"
	@echo "  clean    - Remove build outputs"

build:
	rm -rf build/
	$(GRADLEW) build

test: build
	$(GRADLEW) test

release: clean build
	$(GRADLEW) jreleaserRelease

publish: clean build
	$(GRADLEW) jreleaserDeploy -PdeployMavenCentral=true

clean:
	$(GRADLEW) clean
