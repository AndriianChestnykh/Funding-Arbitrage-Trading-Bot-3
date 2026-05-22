BINARY   := fab-bot
LINUX    := GOOS=linux GOARCH=amd64 CGO_ENABLED=0

.PHONY: build run test lint deploy

build:
	go build -o $(BINARY) ./cmd/fab-bot

build-linux:
	$(LINUX) go build -o $(BINARY)-linux ./cmd/fab-bot

run:
	go run ./cmd/fab-bot

test:
	go test ./... -race -count=1

lint:
	golangci-lint run ./...

deploy: build-linux
	@echo "Deploying to VPS..."
	scp $(BINARY)-linux $(VPS_USER)@$(VPS_HOST):/opt/fab-bot/fab-bot
	ssh $(VPS_USER)@$(VPS_HOST) "systemctl restart fab-bot && systemctl status fab-bot --no-pager"

backup-db:
	ssh $(VPS_USER)@$(VPS_HOST) "cp /opt/fab-bot/bot.db /opt/fab-bot/bot.db.bak"

logs:
	ssh $(VPS_USER)@$(VPS_HOST) "journalctl -fu fab-bot"
