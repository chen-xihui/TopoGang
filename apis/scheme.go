// Package apis 聚合 TopoGang 自定义 API 组的 scheme 注册。
package apis

import (
	"k8s.io/apimachinery/pkg/runtime"

	schedulingv1alpha1 "github.com/chenxihui/TopoGang/apis/scheduling/v1alpha1"
	topologyv1alpha1 "github.com/chenxihui/TopoGang/apis/topology/v1alpha1"
)

// AddToScheme 将全部 TopoGang 自定义资源注册到 scheme。
func AddToScheme(s *runtime.Scheme) error {
	if err := schedulingv1alpha1.AddToScheme(s); err != nil {
		return err
	}
	return topologyv1alpha1.AddToScheme(s)
}
