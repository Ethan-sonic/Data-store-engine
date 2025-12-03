package WritePath

type Memtable struct {
	Data *SkipList
	Size uint64
	Trashold uint8
	CurrSize uint64
	Generation uint32
}

type Config struct {
	WalSize uint64  	`yaml:"wal_size"`
	MemtableSize uint64	`yaml:"memtable_size"`
	Trashold uint8 	`yaml:"trashold"`
}

func NewMemtable(size uint64, trashold uint8) *Memtable {
	mem := Memtable{}

	mem.Data = createSkipList()
	mem.Size = size
	mem.Trashold = trashold
	mem.CurrSize = 0
	mem.Generation = 0

	return &mem
}

func (mem *Memtable) Add(key string, value []byte) {

	mem.Data.addNode(key, value)
	mem.CurrSize += 1

}

func (mem *Memtable) Find(key string) ([]byte, bool) {
	node, err := mem.Data.findNode(key)
	if err != nil {
		return nil, false
	} else {
		return node.Value, true
	}
}

func (mem *Memtable) IsFull() bool {
	return uint8(mem.CurrSize*100/mem.Size) >= mem.Trashold
}

func (mem *Memtable) Delete(key string, value []byte) {
	mem.Data.deleteNode(key, value)
}

func (mem *Memtable) Flush() {
	mem.CurrSize = 0
	mem.Generation += 1

	mem.Data = createSkipList()
}
