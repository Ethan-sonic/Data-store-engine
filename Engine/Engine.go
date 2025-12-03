package Engine

import (
	"NASP_projekat/Methods"
	"NASP_projekat/Structures"
	"NASP_projekat/WritePath"
	"encoding/binary"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"math"
	"os"
)

type Config struct {
	WalSize      int    `yaml:"wal_size"`
	WalBuff      int    `yaml:"wal_buff"`
	LowWaterMark int    `yaml:"wal_lowWaterMark"`
	SegMax       int    `yaml:"wal_segMax"`
	MemtableSize uint64 `yaml:"memtable_size"`
	Trashold     uint8  `yaml:"trashold"`
	CacheSize    uint32 `yaml:"cache_size"`
	LSMLevels    uint8  `yaml:"LSM_levels"` // broj nivoa LSM stabla
	LevelCap     uint8  `yaml:"level_cap"`  // koliko sstable-a moze da stane u jedan nivo (osim poslednjeg)
	Limit        int    `yaml:"limit"`
	Rate         int    `yaml:"rate"`
}

type Engine struct {
	Wal         *WritePath.WAL
	Memtable    *WritePath.Memtable
	Cache       *WritePath.Cache
	RateLimit   *WritePath.RateLimiting
	LSMLevels   uint8
	LevelCap    uint8
	Generations []uint8
	CMS *Structures.MyCountMinSketch
	HLL *Structures.HLL
}

func GetConfiguration() Config {
	var config Config
	if _, err := os.Stat("data/config.yml"); err != nil {
		config = Config{3, 3, 4, 6, 10, 70, 5, 3, 4, 500, 4}
	} else {
		configData, err := ioutil.ReadFile("data/config.yml")
		if err != nil {
			fmt.Println(err)
		}
		yaml.Unmarshal(configData, &config)
	}
	//config = Config{3, 3, 4, 6, 10, 70, 5, 3, 4, 500, 4}
	return config
}

func NewEngine() *Engine {
	config := GetConfiguration()
	engine := Engine{LSMLevels: config.LSMLevels, LevelCap: config.LevelCap}
	engine.Generations = make([]uint8, engine.LSMLevels)
	engine.Wal = WritePath.NewWAL(config.WalSize, 6, config.WalSize, config.LowWaterMark)
	engine.Memtable = WritePath.NewMemtable(config.MemtableSize, config.Trashold)
	engine.Cache = WritePath.NewCache(config.CacheSize)
	engine.RateLimit = WritePath.CreateRateLimiting(500, 4)
	engine.CMS = Structures.NewMyCountMinSketch(0.001, 0.001)
	engine.CMS.Serialize("CMS.gob")
	engine.HLL = Structures.NewHLL(8)
	engine.HLL.Serialize("HLL.gob")
	return &engine
}

func (engine *Engine) PUTCMS(key []byte) {
	engine.CMS.Deserialize("CMS.gob")
	engine.CMS.AddKey(key)
	engine.CMS.Serialize("CMS.gob")
}

func (engine *Engine) PUTHLL(key string) {
	engine.HLL.Deserialize("HLL.gob")
	engine.HLL.Add(key)
	engine.HLL.Serialize("HLL.gob")
}

func (engine *Engine) Put(key string, value []byte) bool {
	if engine.RateLimit.IsAllowed() {
		bytes := engine.Wal.Add(key, value)
		engine.PUTCMS([]byte(key))
		engine.PUTHLL(key)
		engine.Memtable.Add(key, bytes)
		if engine.Memtable.IsFull() {
			engine.Flush()
			return true
		}
	}
	return false
}

func (engine *Engine) Get(key string) []byte {
	value := engine.SearchForValue(key)
	if value != nil && value[12] == 0 {
		engine.Cache.Insert(key, value)
		return WritePath.ByteToRec(value).Value
	} else {
		fmt.Printf("Podatak sa klucem %s nije pronadjen.", key)
		return nil
	}
}

func (engine *Engine) Delete(key string) bool {
	if engine.RateLimit.IsAllowed() {
		bytes := engine.Wal.Remove(key)
		engine.Cache.Delete(key)
		engine.Memtable.Delete(key, bytes)
		if engine.Memtable.IsFull() {
			engine.Flush()
		}
		return true
	}
	return false
}

func (engine *Engine) SearchBloomFilter(path string, key string) bool {
	bloomfilter := Structures.MyBloomFilter{}
	bloomfilter.Deserialize(path)
	return bloomfilter.Exist(key)
}

func (engine *Engine) SearchSummary(path string, key string) (uint32, bool) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
	}

	key1, _ := Methods.ReadStringBytes(file)
	key2, _ := Methods.ReadStringBytes(file)
	if key < string(key1) || key > string(key2) {
		return 0, false
	}

	for {
		currKey, _ := Methods.ReadStringBytes(file)
		if key <= string(currKey) {
			offset, _ := Methods.ReadUint32(file)
			return offset, true
		}
		file.Seek(4, 1)
	}
}

func (engine *Engine) SearchIndex(path string, key string, offset uint32) (uint32, bool) {

	file, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
	}
	_, _ = file.Seek(int64(offset), 0)

	for {
		keySizeBytes := make([]byte, 4)
		_, err = file.Read(keySizeBytes)
		if err != nil {
			break
		}
		keySize := binary.LittleEndian.Uint32(keySizeBytes)

		keyBytes := make([]byte, keySize)
		_, _ = file.Read(keyBytes)
		if string(keyBytes) == key {
			offsetBytes := make([]byte, 4)
			_, _ = file.Read(offsetBytes)
			return binary.LittleEndian.Uint32(offsetBytes), true
		} else {
			_, _ = file.Seek(4, 1)
		}
	}

	return 0, false
}

func ReadRecord(file *os.File) []byte {

	recRead := make([]byte, 21)
	_, err := file.Read(recRead)
	if err != nil {
		return nil
	}

	crc := binary.LittleEndian.Uint32(recRead[:])
	//timestamp := binary.LittleEndian.Uint64(recRead[4:])
	if tombstone := recRead[12]; tombstone == 1 {
		return nil
	}
	keySize := int(binary.LittleEndian.Uint32(recRead[13:]))
	valSize := int(binary.LittleEndian.Uint32(recRead[17:]))

	key, value := make([]byte, keySize), make([]byte, valSize)
	if _, err = file.Read(key); err != nil {
		return nil
	}
	if _, err = file.Read(value); err != nil {
		return nil
	}

	if WritePath.CRC32(value) == crc {
		recRead = append(recRead, key...)
		recRead = append(recRead, value...)
		return recRead
	} else {
		return nil
	}

}

func (engine *Engine) FindFromData(path string, offset uint32) []byte {

	file, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
	}

	file.Seek(int64(offset), 0)
	value := ReadRecord(file)
	return value

}

func (engine *Engine) SearchForValue(key string) []byte {
	var value []byte
	var found bool
	if value, found = engine.Memtable.Find(key); found == true {
		return value
	}
	if value, found = engine.Cache.Search(key); found == true {
		return value
	}

	var path1, path2, path3, path4 string
	for lvl := uint32(1); lvl <= 3; lvl++ {
		for gen := engine.Generations[lvl-1]; gen >= 1; gen-- {
			path1 = fmt.Sprintf("data/sstable/%d-usertable-%d-bloom.gob", lvl, gen)
			path2 = fmt.Sprintf("data/sstable/%d-usertable-%d-summary.binary", lvl, gen)
			path3 = fmt.Sprintf("data/sstable/%d-usertable-%d-index.binary", lvl, gen)
			path4 = fmt.Sprintf("data/sstable/%d-usertable-%d-data.binary", lvl, gen)
			if _, err := os.Stat(path1); err != nil {
				continue
			}

			if found = engine.SearchBloomFilter(path1, key); found == false {
				continue
			}
			if offset1, found := engine.SearchSummary(path2, key); found == true {
				if offset2, found := engine.SearchIndex(path3, key, offset1); found == true {
					return engine.FindFromData(path4, offset2)
				}
			}
		}
	}

	return nil
}

func (engine *Engine) Flush() {
	engine.Generations[0] += 1
	engine.NewSSTable(engine.Memtable.Data, engine.Generations[0])
	engine.Memtable.Flush()
	engine.Wal.Delete()
}

func (engine *Engine) NewSSTable(data *WritePath.SkipList, generation uint8) {
	bloom := Structures.NewBloomFilter(int(engine.Memtable.Size), 99)
	dataElements := make([]byte, 0)
	indexElements := make([]byte, 0)
	summaryElements := make([]byte, 0)
	summaryInterval := make([]byte, 0)
	merkleElements := make([][]byte, 0)
	offsetIdx := 0
	offsetSumm := 0
	n := int(math.Ceil(float64(data.Size) * 0.1))

	dataFilename := fmt.Sprintf("data/sstable/1-usertable-%d-data.binary", generation)
	indexFilename := fmt.Sprintf("data/sstable/1-usertable-%d-index.binary", generation)
	summaryFilename := fmt.Sprintf("data/sstable/1-usertable-%d-summary.binary", generation)
	bloomFilename := fmt.Sprintf("data/sstable/1-usertable-%d-bloom.gob", generation)
	merkleFilename := fmt.Sprintf("data/sstable/1-usertable-%d-Metadata.txt", generation)

	dataFile, _ := os.OpenFile(dataFilename, os.O_RDWR|os.O_CREATE, 0777)
	defer dataFile.Close()
	indexFile, _ := os.OpenFile(indexFilename, os.O_RDWR|os.O_CREATE, 0777)
	defer indexFile.Close()
	summaryFile, _ := os.OpenFile(summaryFilename, os.O_RDWR|os.O_CREATE, 0777)
	defer summaryFile.Close()
	TOCFile, _ := os.OpenFile("data/sstable/TOC.txt", os.O_RDWR|os.O_CREATE, 0777)
	defer TOCFile.Close()
	TOCFile.Seek(0, 2)

	curr := data.Head
	for curr.Lower != nil {
		curr = curr.Lower
	}
	curr = curr.Next

	Methods.AppendKey(&summaryInterval, curr.Value)
	i := 1
	for {
		bloom.AddElement(curr.Key)
		dataElements = append(dataElements, curr.Value...)

		keySize := binary.LittleEndian.Uint32(curr.Value[13:17])
		Methods.AppendKeyOffset(&indexElements, curr.Value, uint32(offsetIdx))
		if i%n == 0 {
			Methods.AppendKeyOffset(&summaryElements, curr.Value, uint32(offsetSumm))
		}
		merkleElements = append(merkleElements, curr.Value)

		offsetIdx += len(curr.Value)
		offsetSumm += 8 + int(keySize)
		i += 1
		if curr.Next == nil {
			Methods.AppendKey(&summaryInterval, curr.Value)
			break
		}
		curr = curr.Next
	}

	bloom.Serialize(bloomFilename)
	indexFile.Write(indexElements)
	indexFile.Close()
	summaryFile.Write(summaryInterval)
	summaryFile.Write(summaryElements)
	summaryFile.Close()
	dataFile.Write(dataElements)
	dataFile.Close()
	TOCFile.Write([]byte(dataFilename + "\n" + indexFilename + "\n" + bloomFilename + "\n"))
	Structures.Rad(merkleElements, merkleFilename)

	for i := uint8(1); i < engine.LSMLevels; i++ {
		if engine.Generations[i-1] >= 4 {
			fmt.Printf("Pokrenut je proces kompakcije za %d. nivo\n", i)
			engine.Compaction(i)
		}
	}
}



func (engine *Engine) Compaction (i uint8) {
	engine.Generations[i-1] = 0
	engine.Generations[i] += 1
	engine.Merge(i, 1, 2, i)
	engine.Merge(i, 3, 4, i)
	engine.Generations[i-1] = 0
	engine.Merge(i, 1, 2, i+1)

}

func UpdateSSTValues(bloomfilter *Structures.MyBloomFilter, idxElems, dataElems *[]byte, key, rec []byte, idxOffset *uint32) {
	bloomfilter.AddElement(string(key))

	Methods.AppendNum(idxElems, uint32(len(key)))
	*idxElems = append(*idxElems, key...)
	Methods.AppendNum(idxElems, *idxOffset)
	*idxOffset += uint32(len(rec))

	*dataElems = append(*dataElems, rec...)
}

func UpdateSummary(summElems *[]byte, key []byte, summOffset *uint32) {
	Methods.AppendNum(summElems, uint32(len(key)))
	*summElems = append(*summElems, key...)
	Methods.AppendNum(summElems, *summOffset)
	*summOffset += 8 + uint32(len(key))
}


func (engine *Engine) CreateNewSSTable(bloomfilter *Structures.MyBloomFilter, idxElems, summInterval, summElems, dataElems *[]byte, merkleElems *[][]byte, newlvl uint8) {
	//engine.Generations[newlvl-1] += 1
	lvl, gen := newlvl, engine.Generations[newlvl-1]
	bloomPath := fmt.Sprintf("data/sstable/%d-usertable-%d-bloom.gob", lvl, gen)
	indexPath := fmt.Sprintf("data/sstable/%d-usertable-%d-index.binary", lvl, gen)
	summPath := fmt.Sprintf("data/sstable/%d-usertable-%d-summary.binary", lvl, gen)
	dataPath := fmt.Sprintf("data/sstable/%d-usertable-%d-data.binary", lvl, gen)
	merklePath := fmt.Sprintf("data/sstable/%d-usertable-%d-Metadata.txt", lvl, gen)

	//fmt.Println("Prilikom kompakcije kreirano: ", lvl, "-", gen)

	bloomfilter.Serialize(bloomPath)
	indexFile, _ := os.OpenFile(indexPath, os.O_RDWR | os.O_CREATE, 0777); defer indexFile.Close()
	summaryFile, _ := os.OpenFile(summPath, os.O_RDWR | os.O_CREATE, 0777); defer summaryFile.Close()
	dataFile, _ := os.OpenFile(dataPath, os.O_RDWR | os.O_CREATE, 0777); defer dataFile.Close()

	indexFile.Write(*idxElems)
	summaryFile.Write(*summInterval)
	summaryFile.Write(*summElems)
	dataFile.Write(*dataElems)
	Structures.Rad(*merkleElems, merklePath)
}

func (engine *Engine) DeleteOldSSTable(lvl, i1, i2 uint8) {
	for i := i1; i <= i2; i++ {
		err := os.Remove(fmt.Sprintf("data/sstable/%d-usertable-%d-bloom.gob", lvl, i)); if err != nil {fmt.Println(err, lvl, i)}
		err = os.Remove(fmt.Sprintf("data/sstable/%d-usertable-%d-index.binary", lvl, i)); if err != nil {fmt.Println(err, lvl, i)}
		err = os.Remove(fmt.Sprintf("data/sstable/%d-usertable-%d-summary.binary", lvl, i)); if err != nil {fmt.Println(err, lvl, i)}
		err = os.Remove(fmt.Sprintf("data/sstable/%d-usertable-%d-data.binary", lvl, i)); if err != nil {fmt.Println(err, lvl, i)}
		err = os.Remove(fmt.Sprintf("data/sstable/%d-usertable-%d-Metadata.txt", lvl, i)); if err != nil {fmt.Println(err, lvl, i)}

		//fmt.Println("Prilikom kompakcije obrisano: ",lvl,"-",i)
	}
}

/*
nivo	generacija
1			1 		1 	1
1			2
1			3		1	2
1			4
*/
func (engine *Engine) Merge(lvl, i1, i2, newlvl uint8) {
	var err1, err2 error
	var key1, key2, key string
	var rec1, rec2, rec, keyBytes, lastKey []byte

	bloomfilter := Structures.NewBloomFilter(int(i2)*int(engine.Memtable.Size), 99)
	idxOffset, summOffset, counter := uint32(0), uint32(0), 0
	n := int(math.Ceil(float64(engine.Memtable.Size) * 0.1))
	idxElems, summInterval, summElems, dataElems, merkleElems := make([]byte, 0), make([]byte, 0), make([]byte, 0), make([]byte, 0), make([][]byte, 0)

	index1, data1 := Methods.OpenSSTFile("index.binary", lvl, i1), Methods.OpenSSTFile("data.binary", lvl, i1)
	index2, data2 := Methods.OpenSSTFile("index.binary", lvl, i2), Methods.OpenSSTFile("data.binary", lvl, i2)

	key1, err1 = Methods.ReadString(index1); key2, err2 = Methods.ReadString(index2)
	for err1 == nil || err2 == nil || key != "" {
		if key != "" && key1 != key && key2 != key && rec[12] == 0 {
			counter += 1
			if counter == 1 { Methods.AppendNum(&summInterval, uint32(len(keyBytes))); summInterval = append(summInterval, key...) }	// Prvi u intervalu
			UpdateSSTValues(&bloomfilter, &idxElems, &dataElems, keyBytes, rec, &idxOffset)
			if counter % n == 0 { UpdateSummary(&summElems, keyBytes, &summOffset)}
			merkleElems = append(merkleElems, rec)
			lastKey = keyBytes
		}
		if err1 != nil && err2 != nil { break }
		if (err1 == nil && (key1 < key2)) || err2 != nil {
			key, keyBytes = key1, []byte(key1);
			rec = ReadRecord(data1)
			key1, err1 = Methods.NextKey(index1)
		} else if (err2 == nil && (key1 > key2)) || err1 != nil {
			key, keyBytes = key2, []byte(key2);
			rec = ReadRecord(data2)
			key2, err2 = Methods.NextKey(index2)
		} else if key1 == key2 && key1 != "" {
			key, keyBytes = key1, []byte(key1)
			rec1, rec2 = ReadRecord(data1), ReadRecord(data2)
			if Methods.Rec1AfterRec2(rec1, rec2) { rec = rec1 } else { rec = rec2 }
			key1, err1 = Methods.NextKey(index1)
			key2, err2 = Methods.NextKey(index2)
		}
	}
	Methods.AppendNum(&summInterval, uint32(len(lastKey))); summInterval = append(summInterval, lastKey...)

	// Kreiraj nove fajlove i obrisi stare
	if lvl == newlvl { engine.Generations[lvl-1] += 1 }
	index1.Close(); data1.Close(); index2.Close(); data2.Close()
	engine.DeleteOldSSTable(lvl, i1, i2)
	engine.CreateNewSSTable(&bloomfilter, &idxElems, &summInterval, &summElems, &dataElems, &merkleElems, newlvl)

}
