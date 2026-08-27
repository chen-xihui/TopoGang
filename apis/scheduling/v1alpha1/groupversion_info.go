// Package v1alpha1 定义 scheduling.topogang.io 的 PodGroup API。
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion 是 GroupVersion 标识。
var GroupVersion = schema.GroupVersion{Group: "scheduling.topogang.io", Version: "v1alpha1"}

// SchemeBuilder 用于向 runtime.Scheme 注册本组资源。
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

func init() {
	SchemeBuilder.Register(&PodGroup{}, &PodGroupList{})
}

// AddToScheme 将本组资源注册到 scheme。
func AddToScheme(s *runtime.Scheme) error {
	return SchemeBuilder.AddToScheme(s)
}
