package agent

import (
	"bufio"
	"context"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// nvidia-smi topo -m 输出示例（§7.1）：
//
//	       GPU0   GPU1   GPU2   GPU3   GPU4   GPU5   GPU6   GPU7
//	GPU0    X     NV1    NV1    NV1    SYS    SYS    SYS    SYS
//	GPU1    NV1    X     NV1    NV1    SYS    SYS    SYS    SYS
//	...
//
// 链路等级（附录 B）：NV1/NV2/NV3 -> NVLink，PIX/PHB -> PIX/PHB，SYS -> SYS。

// NvidiaSmiSource 通过 `nvidia-smi topo -m` 采集 GPU 拓扑（§7.1）。
type NvidiaSmiSource struct {
	// Command 可注入的 nvidia-smi 路径（默认 "nvidia-smi"）。
	Command string
	// Exec 可注入的命令执行器（测试用）。
	Exec func(ctx context.Context, name string, args ...string) ([]byte, error)
}

// NewNvidiaSmiSource 构造默认的 nvidia-smi 采集源。
func NewNvidiaSmiSource() *NvidiaSmiSource {
	return &NvidiaSmiSource{
		Command: "nvidia-smi",
		Exec:    defaultExec,
	}
}

func defaultExec(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.Output()
}

// Discover 执行 nvidia-smi topo -m 并解析为 GpuTopology。
func (s *NvidiaSmiSource) Discover(ctx context.Context) (*topo.GpuTopology, error) {
	out, err := s.Exec(ctx, s.Command, "topo", "-m")
	if err != nil {
		return nil, err
	}
	return parseNvidiaSmiTopo(out)
}

// parseNvidiaSmiTopo 解析 `nvidia-smi topo -m` 文本输出。
func parseNvidiaSmiTopo(out []byte) (*topo.GpuTopology, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	var header []string
	var rows [][]string
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(header) == 0 {
			// 首行是表头：GPU0 GPU1 ...
			header = fields
			continue
		}
		if len(fields) > 0 && strings.HasPrefix(fields[0], "GPU") {
			rows = append(rows, fields[1:])
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	gpuCount := len(header)
	// 兼容表头可能是 GPU0... 实际索引，rows 顺序应与 header 对应
	gpuIndexes := make([]int, 0, gpuCount)
	for _, h := range header {
		idx := gpuIdxFromName(h)
		if idx < 0 {
			// 非 GPU 列（如 "GPU" 全称），退回顺序索引
			idx = len(gpuIndexes)
		}
		gpuIndexes = append(gpuIndexes, idx)
	}

	g := &topo.GpuTopology{GPUs: make([]*topo.Gpu, 0, gpuCount)}
	for _, idx := range gpuIndexes {
		g.GPUs = append(g.GPUs, &topo.Gpu{Index: idx, ID: "GPU" + strconv.Itoa(idx)})
	}

	seen := map[[2]int]bool{}
	for i := 0; i < len(rows); i++ {
		row := rows[i]
		if len(row) < gpuCount {
			continue
		}
		a := gpuIndexes[i]
		for j := 0; j < gpuCount; j++ {
			if i == j {
				continue
			}
			b := gpuIndexes[j]
			link := parseLinkType(row[j])
			if link == "" {
				continue
			}
			// 无向边去重
			key := [2]int{minInt(a, b), maxInt(a, b)}
			if seen[key] {
				continue
			}
			seen[key] = true
			g.Links = append(g.Links, &topo.Link{
				A:        a,
				B:        b,
				LinkType: link,
				Bandwidth: topo.LinkBandwidth(link),
			})
		}
	}
	// 保证 Links 确定性顺序
	sort.Slice(g.Links, func(i, j int) bool {
		if g.Links[i].A != g.Links[j].A {
			return g.Links[i].A < g.Links[j].A
		}
		return g.Links[i].B < g.Links[j].B
	})
	return g, nil
}

func gpuIdxFromName(name string) int {
	// 期望 "GPU0"、"GPU10" 等
	if !strings.HasPrefix(name, "GPU") {
		return -1
	}
	v, err := strconv.Atoi(strings.TrimPrefix(name, "GPU"))
	if err != nil {
		return -1
	}
	return v
}

func parseLinkType(s string) topo.LinkType {
	switch {
	case s == "X":
		return ""
	case s == "NVSWITCH" || strings.Contains(s, "NVSWITCH"):
		return topo.LinkNVSwitch
	case strings.HasPrefix(s, "NV"):
		return topo.LinkNVLink
	case s == "PIX":
		return topo.LinkPIX
	case s == "PHB":
		return topo.LinkPHB
	case s == "SYS":
		return topo.LinkSYS
	default:
		return ""
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
