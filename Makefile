PYTHON ?= python3

.PHONY: test test-controller test-simulation simulation controller-build

test: test-controller test-simulation

test-controller:
	cd controller && go test ./...

test-simulation:
	$(PYTHON) -m pytest -q

simulation:
	$(PYTHON) simulation/app.py

controller-build:
	cd controller && CGO_ENABLED=0 go build -trimpath -o sandbox-pool-controller .
