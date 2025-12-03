package WritePath

import (
	"container/list"
)

type Cache struct {
	Map      map[string]*list.Element
	List     *list.List
	Capacity uint32
	currSize uint32
}

type ListNode struct {
	Key   string
	Value []byte
}

func NewCache(capacity uint32) *Cache {
	cache := Cache{Capacity: capacity}
	cache.Map = make(map[string]*list.Element)
	cache.List = list.New()
	cache.currSize = 0
	return &cache
}

func (cache *Cache) Insert(key string, value []byte) {
	if element, exists := cache.Map[key]; exists == true {
		cache.List.MoveToFront(element)
	} else {
		element = cache.List.PushFront(&ListNode{key, value})
		cache.Map[key] = element
		if cache.currSize >= cache.Capacity {
			listInterface := cache.List.Remove(cache.List.Back())
			listNode := listInterface.(*ListNode)
			delete(cache.Map, listNode.Key)
		} else {
			cache.currSize += 1
		}
	}
}

func (cache *Cache) Search(key string) ([]byte, bool) {
	if element, exists := cache.Map[key]; exists == true {
		listNode, ok := element.Value.(*ListNode)
		if !ok {
			return nil, false
		}
		return listNode.Value, true
	}
	return nil, false
}

func (cache *Cache) Delete(key string) {
	if element, exists := cache.Map[key]; exists == true {
		cache.List.Remove(element)
		delete(cache.Map, key)
		cache.currSize -= 1
	}
}
