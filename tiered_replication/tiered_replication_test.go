package tiered_replication

import "testing"

func TestBuildCopysets(t *testing.T) {
	nodes := make([]*Node, 0)
	for i := 0; i < 5; i++ {
		node := NewNode(i, "primary")
		nodes = append(nodes, node)
	}
	for i := 5; i < 6; i++ {
		node := NewNode(i, "backup")
		nodes = append(nodes, node)
	}

	cluster := NewCluster(nodes, 2, 1)
	cluster.BuildCopysets()
}

func BenchmarkBuildCopysets(b *testing.B) {
	for i := 0; i < b.N; i++ {
		nodes := make([]*Node, 0)
		for i := 0; i < 2; i++ {
			node := NewNode(i, "primary")
			nodes = append(nodes, node)
		}
		for i := 2; i < 3; i++ {
			node := NewNode(i, "backup")
			nodes = append(nodes, node)
		}

		cluster := NewCluster(nodes, 2, 1)
		cluster.BuildCopysets()
	}
}
