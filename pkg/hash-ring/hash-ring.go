package hashring

import (
	"crypto/sha1"
	"fmt"
	"sort"
)

type HashRing struct {
	nodes       []int     
	nodeMap     map[int]string
}

func NewHashRing() *HashRing {
	return &HashRing{
		nodes:   []int{},
		nodeMap: make(map[int]string),
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
	r.nodes = append(r.nodes, h)
	r.nodeMap[h] = node
	sort.Ints(r.nodes) 
}

func (r *HashRing) GetNode(key string) string {

	if len(r.nodes) == 0 {
		return ""
	}
	h := Hash(key)

	//binary search, clockwise movement on ring
	idx := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i] >= h
	})
	if idx == len(r.nodes) {
		idx = 0 
	}

	return r.nodeMap[r.nodes[idx]]
}
