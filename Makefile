# TopoGang Makefile
#
# 里程碑目标（见 docs/DESIGN.md §13）：build / test / gen-crd / run-agent / run-controller。

BIN_DIR := bin
CONTROLLER_GEN := $(shell command -v controller-gen 2>/dev/null || echo "$(HOME)/go/bin/controller-gen")

.PHONY: all build test vet gen-crd run-agent run-controller clean

all: build test vet

build:
	@echo ">> building all binaries"
	go build -o $(BIN_DIR)/topo-agent.exe ./cmd/agent
	go build -o $(BIN_DIR)/topogang-controller.exe ./cmd/controller
	go build -o $(BIN_DIR)/topogang-scheduler.exe ./cmd/scheduler

# 校验 scheduler 配置
check-scheduler:
	go run ./cmd/scheduler --config=config/scheduler/scheduler-config.yaml

test:
	@echo ">> running unit tests"
	go test ./... -count=1

vet:
	@echo ">> running go vet"
	go vet ./...

# 生成 CRD YAML（需 controller-gen，可 go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.14.0）
gen-crd:
	$(CONTROLLER_GEN) crd:allowDangerousTypes=true paths="github.com/chenxihui/TopoGang/apis/..." output:crd:artifacts:config=config/crd/bases

# 运行 topo-agent（mock 源，8 卡双域），验证采集→域划分→写入链路。
# 用法：make run-agent  （Ctrl+C 退出）
run-agent:
	go run ./cmd/agent --node-name=node-a --source=mock --mock-spec=8-2 --interval=5s -v=2

clean:
	rm -rf $(BIN_DIR)
