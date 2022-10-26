package tiered_replication

import (
	"container/list"
	"sort"
)

type Tier = string

const backup Tier = "backup"

type Copyset struct {
	nodes *list.List
}

func NewCopyset() *Copyset {
	return &Copyset{
		nodes: list.New(),
	}
}

func (c *Copyset) Size() int {
	return c.nodes.Len()
}

func (c *Copyset) AddNode(n *Node) {
	c.nodes.PushBack(n)
}

func (c *Copyset) RemoveNode(n *Node) {
	for nodeIter := c.nodes.Front(); nodeIter != nil; nodeIter = nodeIter.Next() {
		node := nodeIter.Value.(*Node)
		if node.id == n.id {
			c.nodes.Remove(nodeIter)
			break
		}
	}
}

func (c *Copyset) Same(copyset *Copyset) bool {
	nodeSet := make(map[NodeID]Empty, 0)
	for iter := c.nodes.Front(); iter != nil; iter = iter.Next() {
		node := iter.Value.(*Node)
		nodeSet[node.id] = Empty{}
	}
	for iter := copyset.nodes.Front(); iter != nil; iter = iter.Next() {
		node := iter.Value.(*Node)
		if _, ok := nodeSet[node.id]; ok {
			delete(nodeSet, node.id)
		}
	}
	// same
	if len(nodeSet) <= 0 {
		return true
	}

	return false
}

type NodeID = int

type Node struct {
	// scatter width
	id       NodeID
	scatterW int
	tier     string
}

func NewNode(id NodeID, tier string) *Node {
	return &Node{
		id:       id,
		scatterW: 0,
		tier:     tier,
	}
}

type Empty = struct{}

type Cluster struct {
	nodes    []*Node
	copysets []*Copyset
	// node's id to copysets
	indexs   map[NodeID][]*Copyset
	scatterW int
	replicas int
}

func NewCluster(nodes []*Node, R int, scatterW int) *Cluster {
	return &Cluster{
		nodes:    nodes,
		copysets: make([]*Copyset, 0),
		indexs:   make(map[NodeID][]*Copyset),
		scatterW: scatterW,
		replicas: R,
	}
}

// 如果超过一个节点来自备份层或者R个节点都来自主层，返回false
// 即保证copyset中的节点R-1个来自主层，1个来自备份层
func (cluster *Cluster) CheckTier(copyset *Copyset) bool {
	backupCnt := 0
	for iter := copyset.nodes.Front(); iter != nil; iter = iter.Next() {
		node := iter.Value.(*Node)
		if node.tier == backup {
			backupCnt++
		}
	}
	if backupCnt > 1 || backupCnt == 0 {
		return false
	}
	return true
}

// 添加copyset到copysets列表中
func (cluster *Cluster) AddCopyset(copyset *Copyset) {
	// add indexs and update scatter width
	for iter := copyset.nodes.Front(); iter != nil; iter = iter.Next() {
		node := iter.Value.(*Node)
		cluster.indexs[node.id] = append(cluster.indexs[node.id], copyset)

		set := make(map[NodeID]Empty, 0)
		copysets := cluster.indexs[node.id]
		for _, copyset := range copysets {
			for nodeIter := copyset.nodes.Front(); nodeIter != nil; nodeIter = nodeIter.Next() {
				n := nodeIter.Value.(*Node)
				if n.id == node.id {
					continue
				}
				set[n.id] = Empty{}
			}
		}
		node.scatterW = len(set)
	}
	// insert to list of copysets
	cluster.copysets = append(cluster.copysets, copyset)

	// visualization
	// fmt.Printf("new copyset: \n")
	// for iter := copyset.nodes.Front(); iter != nil; iter = iter.Next() {
	// 	node := iter.Value.(*Node)
	// 	fmt.Printf("Node: %v sw: %v Tier: %v\n", node.id, node.scatterW, node.tier)
	// }
}

// 如果每一个节点都不与其他节点一起出现在之前的copysets中（即生成的copyset不与之前的重复），则返回true
func (cluster *Cluster) DidNotAppear(copyset *Copyset) bool {
	for iter := copyset.nodes.Front(); iter != nil; iter = iter.Next() {
		node := iter.Value.(*Node)
		copysets := cluster.indexs[node.id]

		for i := 0; i < len(copysets); i++ {
			if copysets[i].Same(copyset) {
				return false
			}
		}
	}

	return true
}

func (cluster *Cluster) Check(copyset *Copyset) bool {
	if cluster.CheckTier(copyset) && cluster.DidNotAppear(copyset) {
		return true
	}
	return false
}

func (cluster *Cluster) Sort() []*Node {
	sort.Slice(cluster.nodes, func(i, j int) bool {
		return cluster.nodes[i].scatterW < cluster.nodes[j].scatterW
	})
	return cluster.nodes
}

func (cluster *Cluster) BuildCopysets() {
	done := false
	for !done {
		done = true
		for i := 0; i < len(cluster.nodes); i++ {
			node := cluster.nodes[i]

			if node.scatterW >= cluster.scatterW {
				continue
			}
			copyset := NewCopyset()
			copyset.AddNode(node)
			sorted := cluster.Sort()

			for j := 0; j < len(sorted); j++ {
				sortedNode := sorted[j]
				if sortedNode.id == node.id {
					continue
				}
				copyset.AddNode(sortedNode)

				if cluster.replicas == copyset.Size() {
					if !cluster.Check(copyset) {
						copyset.RemoveNode(sortedNode)
					} else if copyset.Size() == cluster.replicas {
						cluster.AddCopyset(copyset)
						done = false
						break
					}
				}
			}
		}
	}
}
