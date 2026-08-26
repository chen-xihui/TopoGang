# TopoGang Makefile
#
# M1 地基阶段目标：build / test / run-agent（见 docs/DESIGN.md §13）。

BIN_DIR := bin

.PHONY: all build test vet run-agent clean

all: build test

build:
	@echo ">> building all binaries"
	go build -o $(BIN_DIR)/topo-agent.exe ./cmd/agent

test:
	@echo ">> running unit tests"
	go test ./... -count=1

vet:
	@echo ">> running go vet"
	go vet ./...

# 运行 topo-agent（mock 源，8 卡双域），验证采集→域划分→写入链路。
# 用法：make run-agent  （Ctrl+C 退出）
run-agent:
	go run ./cmd/agent --node-name=node-a --source=mock --mock-spec=8-2 --interval=5s -v=2

clean:
	rm -rf $(BIN_DIR)
