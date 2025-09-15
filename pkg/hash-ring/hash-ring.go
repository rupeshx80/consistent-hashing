package hashring

import (
	"crypto/sha1"
	"fmt"
	"log"
	"sort"
)

type HashRing struct {
	nodes       []int     
	nodeMap     map[int]string
	replicas int
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		nodes:   []int{},
		nodeMap: make(map[int]string),
		replicas: replicas,
	}
}

//Note: switch to murmurHash or FNV for speed later
func Hash(s string) int {
	h := sha1.New()
	h.Write([]byte(s))
	bs := h.Sum(nil)
	return int((uint32(bs[0])<<24 | uint32(bs[1])<<16 | uint32(bs[2])<<8 | uint32(bs[3])))
}

func (r *HashRing) AddNode(node string) {

     h := Hash(node)
    fmt.Printf("Adding server %s at position %d\n", node, h)

	for i:= 0; i< r.replicas; i++{
        vNode := fmt.Sprintf("%s#%d", node, i)
        vh := Hash(vNode)

        log.Printf("Adding virtual node %s at position %d", vNode, vh)

        r.nodes = append(r.nodes, vh)
        r.nodeMap[vh] = node
	}
	sort.Ints(r.nodes) 
}


func (r *HashRing) GetNode(key string) (string, string) {

	if len(r.nodes) == 0 {
		return "", ""
	}
	h := Hash(key)

	//binary search, clockwise movement on ring
	idx := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i] >= h
	})
	if idx == len(r.nodes) {
		idx = 0 
	}

	vnodeHash := r.nodes[idx]
    realNode := r.nodeMap[vnodeHash]

    // vnode is just hash number, but we can display like "hash->realNode"
    vnodeID := fmt.Sprintf("VNode[%d]", vnodeHash)

    return vnodeID, realNode
}

