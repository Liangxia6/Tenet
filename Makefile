PYTHON ?= python3

.PHONY: test go-test python-test

test: go-test python-test

go-test:
	cd go && go test ./...

python-test:
	cd python && $(PYTHON) -m unittest discover -s tests -p 'test_*.py'
