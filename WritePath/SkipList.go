package WritePath

import (
	"fmt"
	"math/rand"
	"time"
)

type Node struct {
	Key   string
	Value []byte
	Level int
	Next  *Node
	Upper *Node
	Lower *Node
}

type SkipList struct {
	Head       *Node
	Height     int
	Max_height int
	Size       int
}

func (skipList *SkipList) sizeOf() int {
	return skipList.Size
}

func createSkipList() *SkipList {
	header := createHead()
	skipList := SkipList{Head: &header, Height: 0, Max_height: 10, Size: 0}
	return &skipList
}

func createHead() Node {
	head := Node{Key: "", Value: nil, Level: 0, Next: nil, Upper: nil, Lower: nil}
	return head
}

func (skipList *SkipList) addNode(key string, value []byte) {
	current := skipList.Head
	skipList.Size++

	for {
		if current.Next == nil {
			if current.Lower != nil {
				current = current.Lower
			} else {
				newNode := Node{Key: key, Value: value, Level: 0, Next: current.Next, Upper: nil, Lower: nil}
				current.Next = &newNode
				reference := &newNode
				addLevel(skipList, reference, key)
				break
			}
		} else if current.Next.Key > key{
			if current.Lower != nil {
				current = current.Lower
			} else {
				newNode := Node{Key: key, Value: value, Level: 0, Next: current.Next, Upper: nil, Lower: nil}
				current.Next = &newNode
				reference := &newNode
				addLevel(skipList, reference, key)
				break
			}
		} else {
			current = current.Next
		}
	}
}

func addLevel(skipList *SkipList, reference *Node, key string) {
	h := 0
	rand.Seed(time.Now().UnixNano())
	for {
		x := rand.Float64()
		//fmt.Println("baceno je", x)

		if h == skipList.Height {
			newHead := createHead()
			newHead.Level = skipList.Head.Level + 1
			skipList.Head.Upper = &newHead
			newHead.Lower = skipList.Head
			skipList.Head = &newHead
			skipList.Height++
		}

		if x < 0.5 {
			break
		} else {
			h++
			newLevel := Node{Key: reference.Key, Value: reference.Value, Level: reference.Level + 1, Next: nil, Upper: nil, Lower: reference}

			for {
				if reference.Next != nil {
					if reference.Next.Upper != nil {
						newLevel.Next = reference.Next.Upper
						break
					} else {
						reference = reference.Next
					}
				} else {
					break
				}
			}
			start := skipList.Head
			for {
				if start.Next != nil {
					if start.Next.Key < key {
						start = start.Next
					} else if start.Next.Key > key && start.Level == newLevel.Level {
						start.Next = &newLevel
						break
					} else if start.Next.Key > key && start.Level != newLevel.Level {
						start = start.Lower
					}
				} else {
					if start.Level == newLevel.Level {
						start.Next = &newLevel
						break
					} else {
						start = start.Lower
					}
				}
			}
			reference = &newLevel
		}
	}
}

func (skipList *SkipList) findNode(key string) (*Node, error) {
	current := skipList.Head
	for {
		if current.Key == key {
			return current, nil
		} else if current.Next != nil {
			if current.Next.Key > key {
				if current.Lower != nil {
					current = current.Lower
				} else {
					return nil, fmt.Errorf("Greska, trazeni kljuc ne postoji.")
				}
			} else {
				current = current.Next
			}
		} else if current.Next == nil {
			if current.Lower != nil {
				current = current.Lower
			} else {
				return nil, fmt.Errorf("Greska, trazeni kljuc ne postoji.")
			}
		}
	}
}

func (skipList *SkipList) deleteNode(key string, value []byte) {
	node, _ := skipList.findNode(key)
	if node != nil {
		node.Value[12] = 1
	} else {
		skipList.addNode(key, value)
	}
}
