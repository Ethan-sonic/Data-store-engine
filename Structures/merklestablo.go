package Structures

import (
	"crypto/sha1"
	"io/ioutil"
	"os"
)

type MerkleTree struct {
	Root  *Node
	Leafs []*Node
}

func Stvaranje(cvorovi []*Node) []*Node {
	nodes := []*Node{}
	if len(cvorovi)%2 == 0 {
		cvorovi = append(cvorovi, &Node{data: []byte{}})
	}
	i := 0
	for {
		if i < len(cvorovi)-2 {
			levo := cvorovi[i]
			desno := cvorovi[i+1]
			podaci := sha1.Sum(append(levo.data[:], desno.data[:]...))
			nodes = append(nodes, &Node{data: podaci[:], left: levo, right: desno})
			i = i + 2
		} else {
			break
		}
	}
	if len(nodes) == 1 {
		return nodes
	} else {
		return Stvaranje(nodes)
	}
}

func Upis(drvo MerkleTree, putanja string) {
	file, err := os.Create(putanja); defer file.Close()
	if err != nil {
		return
	}
	for i := 0; i < len(drvo.Leafs); i++ {
		_ = ioutil.WriteFile(putanja, drvo.Leafs[i].data, 0666)
	}
}

func Rad(podaci [][]byte, putanja string) {

	cvorovi := datatonode(podaci)
	hashevi := Stvaranje(cvorovi)
	drvo := MerkleTree{Leafs: hashevi, Root: hashevi[len(hashevi)-1]}
	Upis(drvo, putanja)
}
