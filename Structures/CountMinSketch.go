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

type MyCountMinSketch struct {
	M       uint
	K       uint
	Table   [][]uint // tabela ucestalosti
	hashSet []hash.Hash32
	Seeds	[]uint
}

func NewMyCountMinSketch(epsilon, delta float64) *MyCountMinSketch {
	cms := MyCountMinSketch{}
	cms.M = CalculateMcms(epsilon)
	cms.K = CalculateKcms(delta)
	cms.Table = Create2DTable(cms.K, cms.M)
	cms.CreateHashFunctions(cms.K)
	return &cms
}

func (cms *MyCountMinSketch) AddKey(key []byte) {
	for i := uint(0); i < cms.K; i++ {
		cms.hashSet[i].Reset()
		_, _ = cms.hashSet[i].Write(key)
		j := cms.hashSet[i].Sum32() % uint32(cms.M)
		cms.Table[i][j] += 1
	}
}

func (cms *MyCountMinSketch) Appearance(key []byte) uint {
	min := uint(math.Inf(1))
	for i := uint(0); i < cms.K; i++ {
		cms.hashSet[i].Reset()
		_, _ = cms.hashSet[i].Write(key)
		j := cms.hashSet[i].Sum32() % uint32(cms.M)
		if cms.Table[i][j] < min {
			min = cms.Table[i][j]
		}
	}
	return min
}

func CalculateMcms(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateKcms(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func (cms *MyCountMinSketch) CreateHashFunctions(k uint) {
	cms.hashSet = []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		cms.hashSet = append(cms.hashSet, murmur3.New32WithSeed(uint32(ts+i)))
		cms.Seeds = append(cms.Seeds, ts+i)
	}
}

func Create2DTable(k, m uint) [][]uint {
	table := make([][]uint, k)
	for i, _ := range table {
		table[i] = make([]uint, m)
	}
	return table
}

func (cms *MyCountMinSketch) Serialize(path string) {
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0777)
	if err != nil {fmt.Println(err)}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(cms)
	if err != nil {fmt.Println(err)}
}

func (cms *MyCountMinSketch) Deserialize(path string) {
	file, err := os.OpenFile(path, os.O_RDWR, 0777)
	if err != nil {fmt.Println(err)}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&cms)
	if err != nil {fmt.Println(err)}

	cms.hashSet = []hash.Hash32{}
	for _, seed := range cms.Seeds {
		cms.hashSet = append(cms.hashSet, murmur3.New32WithSeed(uint32(seed)))
	}
}
