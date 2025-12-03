package Structures

import (
	"crypto/sha1"
	"encoding/hex"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func datatonode(podaci [][]byte) []*Node {
	nodes := make([]*Node, len(podaci))
	for i := 0; i < len(podaci); i++ {
		nodes[i] = &Node{data: []byte(podaci[i])}
	}
	return nodes
}
