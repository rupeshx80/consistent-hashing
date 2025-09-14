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

// 2025/09/10 16:26:10 Database Connected successfully.
// Adding server :6001 at position 1776107796
// Adding server :6002 at position 764740292
// Adding server :6003 at position 2431217217
// 2025/09/10 16:26:10 Key 'sigma' hashed to 2454838649 goes to :6002
// 2025/09/10 16:26:10 Key 'amit' hashed to 1991344932 goes to :6003
// 2025/09/10 16:26:10 Key 'rupesh' hashed to 1409592768 goes to :6001
// 2025/09/10 16:26:10 Key 'deepak' hashed to 3507586613 goes to :6002
// 2025/09/10 16:26:10 Key 'roshan' hashed to 4126137636 goes to :6002
// 2025/09/10 16:26:10 Key 'devid' hashed to 2230610889 goes to :6003
// 2025/09/10 16:26:10 Main server running on :5000