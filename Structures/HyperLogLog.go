package Structures

import (
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"strconv"
	"time"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	M     uint64      // broj baketa
	P     uint8       // prvih P bitova uzimamo za odredjivanje baketa
	Reg   []uint8     // baketi
	hashf hash.Hash32 // hash funkcija
	Seed uint
}


func NewHLL(p uint8) *HLL {
	hll := HLL{P: p}
	hll.M = uint64(math.Pow(2, float64(p)))
	hll.Reg = make([]uint8, hll.M)
	hll.CreateHashFunction()
	return &hll
}

func (hll *HLL) Add(key string) {
	// ------------------------
	hll.hashf.Reset()
	hll.hashf.Write([]byte(key))
	val := hll.hashf.Sum32()
	bin := strconv.FormatInt(int64(val), 2)
	// ---------------------------------------
	//bin := ToBinary(GetMD5Hash(key))

	index := uint64(0)
	for i := uint8(0); i < hll.P; i++ {
		if string(bin[i]) == "1" { index += uint64(math.Pow(2, float64(hll.P-i-1)))}
	}

	zeroes := uint8(0)
	for i := len(bin)-1; string(bin[i]) == "0"; i-- { zeroes++ }

	hll.Reg[index] = zeroes
	//fmt.Println("Reg[", index, "] = ", zeroes)
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func (hll *HLL) EmptyCount() uint8 {
	sum := uint8(0)
	for _, val := range hll.Reg {
		if val == 0 { sum+=1 }
	}
	return sum
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum = sum + math.Pow(float64(-val), 2.0)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.EmptyCount()
	if estimation < 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) CreateHashFunction() {
	ts := uint(time.Now().Unix())
	hll.hashf = murmur3.New32WithSeed(uint32(ts+1))
	hll.Seed = ts
}

func (hll *HLL) Serialize(path string) {
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0777)
	if err != nil {fmt.Println(err)}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(hll)
	if err != nil {fmt.Println(err)}
}

func (hll *HLL) Deserialize(path string) {
	file, err := os.OpenFile(path, os.O_RDWR, 0777)
	if err != nil {fmt.Println(err)}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&hll)
	if err != nil {fmt.Println(err)}

	hll.hashf = murmur3.New32WithSeed(uint32(hll.Seed))
}


