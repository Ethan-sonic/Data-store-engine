package Structures

import (
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"time"
)

type MyBloomFilter struct {
	Bits    []byte
	hashSet []hash.Hash32
	Seeds	[]uint
}

func NewBloomFilter (expectedElements int, percentage float64) MyBloomFilter {
	bloomFilter := MyBloomFilter{}
	m := CalculateM(expectedElements, percentage)
	k := CalculateK(expectedElements, m)
	bloomFilter.Bits = make([]byte, m)
	bloomFilter.CreateHashFunctions(k)
	return bloomFilter
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func (b *MyBloomFilter) CreateHashFunctions(k uint){
	b.hashSet = []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		b.hashSet = append(b.hashSet, murmur3.New32WithSeed(uint32(ts+i)))
		b.Seeds = append(b.Seeds, ts+i)
	}
}

func (b *MyBloomFilter) AddElement(key string) {
	for _, hashf := range b.hashSet {
		hashf.Reset()
		_, _ = hashf.Write([]byte(key))
		i := hashf.Sum32() % uint32(len(b.Bits))
		b.Bits[i] = 1
	}
}

func (b *MyBloomFilter) Exist(key string) bool {
	for _, hashf := range b.hashSet {
		hashf.Reset()
		_, _ = hashf.Write([]byte(key))
		i := hashf.Sum32() % uint32(len(b.Bits))
		if b.Bits[i] == 0 { return false }
	}
	return true
}

func (b *MyBloomFilter) Serialize(path string) {
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0777)
	if err != nil {fmt.Println(err)}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(b)
	if err != nil {fmt.Println(err)}
}

func (b *MyBloomFilter) Deserialize(path string) {

	file, err := os.OpenFile(path, os.O_RDWR, 0777)
	if err != nil {fmt.Println(err)}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&b)
	if err != nil {fmt.Println(err)}

	b.hashSet = []hash.Hash32{}
	for _, seed := range b.Seeds {
		b.hashSet = append(b.hashSet, murmur3.New32WithSeed(uint32(seed)))
	}

}
