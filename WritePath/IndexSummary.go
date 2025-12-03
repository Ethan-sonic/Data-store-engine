package WritePath
//
//import (
//	"encoding/binary"
//	"fmt"
//)
//
//type IndexSummary struct  {
//	entries []Entry
//	minIndexSize uint32
//	maxIndexSize uint32
//	minIndex string
//	maxIndex string
//}
//
//type Entry struct {
//	keySize uint32
//	key string
//	offset uint32
//}
//
//func createIndexSummary(skipList *SkipList) IndexSummary{
//	indexSummary := IndexSummary{entries: []Entry(nil), minIndex: "", maxIndex: ""}
//	current := skipList.head
//	for {
//		if current.lower != nil {
//			current = current.lower
//		} else {
//			break
//		}
//	}
//	offset := 0
//	current = current.next
//	indexSummary.minIndex = current.key
//	indexSummary.minIndexSize = binary.LittleEndian.Uint32(current.value[13:17])
//	indexSummary.entries = append(indexSummary.entries,  Entry{keySize: binary.LittleEndian.Uint32(current.value[13:17]), key: current.key, offset: uint32(offset)})
//
//	for {
//		indexSummary.entries = append(indexSummary.entries, Entry{keySize: binary.LittleEndian.Uint32(current.value[13:17]), key: current.key, offset: uint32(offset)})
//		for i:= 0; i < 10; i++ {
//			offset++
//			if current.next != nil {
//				current = current.next
//			} else {
//				indexSummary.maxIndex = current.key
//				indexSummary.maxIndexSize = binary.LittleEndian.Uint32(current.value[13:17])
//				break
//			}
//		}
//		if current.next == nil {
//			indexSummary.maxIndex = current.key
//			indexSummary.maxIndexSize = binary.LittleEndian.Uint32(current.value[13:17])
//			break
//		}
//	}
//	return indexSummary
//}
//
//func (indexSummary *IndexSummary) isInBounds(key string) bool {
//	if key >= indexSummary.minIndex && key <= indexSummary.maxIndex {
//		return true
//	} else {
//		return false
//	}
//}
//
//func (indexSummary *IndexSummary) getOffset(key string) (uint32,error) {
//	previous := indexSummary.entries[0]
//	for _, entry :=  range indexSummary.entries {
//		if entry.key == key {
//			return entry.offset, nil
//		} else if entry.key > key && previous.key < key {
//			return previous.offset, nil
//		} else {
//			previous = entry
//		}
//	}
//	return 0, fmt.Errorf("Doslo je do greske.")
//}