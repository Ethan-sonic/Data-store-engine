package WritePath

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (4B) | Value Size (4B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a Value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type Record struct {
	CRC uint32
	Timestamp uint64
	Tombstone byte
	KeySize uint32
	ValueSize uint32
	Key string
	Value []byte
}

type WAL struct {
	segCap int
	lowWaterMark int
	segMax int
	segments []string
	active string
	lastNum int
	written int
	content []byte
	buffCap int
	buffWritten int
}

func (wal *WAL) InitalWal() {
	wal.lastNum = 1
	wal.active = "data/wal/wal_00001.binary"
	wal.segments = append(wal.segments, wal.active)
	wal.written = 0
	wal.buffWritten = 0
}

func (wal *WAL) scanDirectory(dirName string) {
	segments := make([]string, 0)
	files, err := ioutil.ReadDir(dirName)
	if err != nil {log.Fatal(err)}

	wal.InitalWal()

	var f os.FileInfo
	for _, f = range files {
		segments = append(segments, fmt.Sprintf("data/wal/%s", f.Name()))
	}
	if f != nil {
		wal.active = f.Name()
		wal.lastNum, _ = strconv.Atoi(f.Name()[4:9])
	}

}

func NewWAL(segcap, segmax, buffcap, lowWaterMark int) *WAL {
	if buffcap > segcap { buffcap = segcap }
	wal := WAL{segCap: segcap, segMax: segmax, lowWaterMark: lowWaterMark, buffCap: buffcap}
	wal.scanDirectory("data/wal")

	return &wal
}

func ByteToRec(recRead []byte) *Record {
	rec := Record{}

	rec.CRC = binary.LittleEndian.Uint32(recRead[:])
	rec.Timestamp = binary.LittleEndian.Uint64(recRead[4:])
	rec.Tombstone = recRead[12]
	rec.KeySize = binary.LittleEndian.Uint32(recRead[13:])
	rec.ValueSize = binary.LittleEndian.Uint32(recRead[17:])
	rec.Key = string(recRead[21:21+rec.KeySize])
	rec.Value = recRead[21+rec.KeySize:]

	return &rec
}

func RecToByte(record *Record) []byte {
	byteRec := make([]byte, 21)

	binary.LittleEndian.PutUint32(byteRec[:], record.CRC)
	binary.LittleEndian.PutUint64(byteRec[4:], record.Timestamp)
	byteRec[12] = 1
	binary.LittleEndian.PutUint32(byteRec[13:], record.KeySize)
	binary.LittleEndian.PutUint32(byteRec[17:], record.ValueSize)
	byteRec = append(byteRec, record.Key...)
	byteRec = append(byteRec, record.Value...)

	return byteRec
}

func getByteArr(key string, value []byte, tombstone byte) []byte {
	crc := CRC32(value)
	timestamp := uint64(time.Now().Unix())
	keySize := uint32(binary.Size([]byte(key)))
	valSize := uint32(binary.Size(value))

	record := make([]byte, 21)
	binary.LittleEndian.PutUint32(record[:], crc)
	binary.LittleEndian.PutUint64(record[4:], timestamp)
	record[12] = tombstone
	binary.LittleEndian.PutUint32(record[13:], keySize)
	binary.LittleEndian.PutUint32(record[17:], valSize)
	record = append(record, key...)
	record = append(record, value...)

	return record
}

func (wal *WAL) writeContent() error {
	file, err := os.OpenFile(wal.active, os.O_RDWR | os.O_CREATE, 0777)
	if err != nil {fmt.Println(err); return err}
	defer file.Close()
	file.Seek(0, 2)
	file.Write(wal.content)
	wal.content = make([]byte, 0)
	wal.buffWritten = 0
	//err = appendFile(file, record)
	if err != nil {fmt.Println(err); return err}

	return nil
}

func (wal *WAL) Write(record []byte) error{
	//err := wal.writeContent(record)
	var err error = nil
	if err == nil {
		wal.written += 1; wal.buffWritten += 1
		wal.content = append(wal.content, record...)
		wal.writeContent()
		if len(wal.segments) >= wal.segMax { wal.Reorganization() }
		//if wal.buffWritten >= wal.buffCap { wal.writeContent() }
		if wal.written == wal.segCap {
			//wal.writeContent()
			wal.written = 0
			wal.lastNum += 1
			wal.active = fmt.Sprintf("data/wal/wal_%05d.binary", wal.lastNum)
			wal.segments = append(wal.segments, wal.active)
		}
		return nil
	}
	return err
}

func (wal *WAL) Add(key string, value []byte) []byte {
	record := getByteArr(key, value, 0)
	err := wal.Write(record)
	if err != nil {fmt.Println(err)}
	return record
}

func (wal *WAL) Remove(key string) []byte {
	record := getByteArr(key, []byte("4"), 1)
	err := wal.Write(record)
	if err != nil {fmt.Println(err)}
	return record
}

func printRecord(file *os.File, start *int) error {
	recRead, err := readRange(file, *start, *start+21)
	if err != nil {return err}

	crc := binary.LittleEndian.Uint32(recRead[:])
	timestamp := binary.LittleEndian.Uint64(recRead[4:])
	tombstone := recRead[12]
	keySize := int(binary.LittleEndian.Uint32(recRead[13:]))
	valSize := int(binary.LittleEndian.Uint32(recRead[17:]))
	key, err := readRange(file, *start+21, *start+21+keySize)
	value, err := readRange(file, *start+21+keySize, *start+21+keySize+valSize)
	*start += 21 + keySize + valSize

	if CRC32(value) == crc {
		fmt.Println(crc, timestamp, tombstone, keySize, valSize, string(key), string(value))
	} else {
		fmt.Println("Greska pri zapisu!")
	}
	return nil
}

func (wal *WAL) Read() {
	file, _ := os.OpenFile(wal.active, os.O_RDWR, 0777)
	defer file.Close()

	start := 0
	for {
		err := printRecord(file, &start)
		if err != nil {break}
	}
}

func (wal *WAL) Reorganization() error {
	if wal.lowWaterMark >= len(wal.segments) {
		wal.lowWaterMark = len(wal.segments) - 1
	}

	for i := 0; i < wal.lowWaterMark; i++ {
		err := os.Remove(wal.segments[i])
		if err != nil {}
	}

	wal.segments = wal.segments[wal.lowWaterMark:]
	if len(wal.segments) == 1 {
		newPath := "data/wal/wal_00001.binary"
		os.Rename(wal.segments[0], newPath)
		wal.segments[0] = newPath
	}
	return nil
}

func (wal *WAL) Delete() {
	DeleteDir("data/wal")
	wal.InitalWal()
}

func DeleteDir(directory string) {
	files, err := ioutil.ReadDir(directory)
	if err != nil {log.Fatal(err)}

	var f os.FileInfo
	for _, f = range files {
		err := os.Remove(fmt.Sprintf("%s/%s",directory, f.Name()))
		if err != nil {fmt.Println(err)}
	}
}

const (
	T_SIZE = 8
	C_SIZE = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}


//func main() {
//
//	wal := NewWAL(3, 2)
//	wal.Write("key1", "value1")
//	wal.Write("key2", "value2")
//	wal.Write("key3", "value3")
//	wal.Write("key4", "value4")
//	wal.Write("key5", "value5")
//	wal.Write("key6", "value6")
//	wal.Write("key7", "value7")
//	wal.Write("key8", "value8")
//	wal.Write("key9", "value9")
//	wal.Write("key10", "value10")
//	wal.Write("key11", "value11")
//	wal.Write("key12", "value12")
//	wal.Write("key13", "value13")
//	wal.Write("key14", "value14")
//	wal.Write("key15", "value15")
//	wal.Write("key16", "value16")
//	wal.Read()
//	wal.Reorganization()
//	//Delete()
//
//}


func appendFile(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	err = file.Truncate(currentLen + int64(len(data)))
	if err != nil { return err }
	//mmapf, err := mmap.MapRegion(file, int(currentLen)+len(data), mmap.RDWR, 0, 0)
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()
	copy(mmapf[currentLen:], data)
	mmapf.Flush()
	return nil
}

// Map maps an entire file into memory

// prot argument
// mmap.RDONLY - Maps the memory read-only. Attempts to write to the MMap object will result in undefined behavior.
// mmap.RDWR - Maps the memory as read-write. Writes to the MMap object will update the underlying file.
// mmap.COPY - Writes to the MMap object will affect memory, but the underlying file will remain unchanged.
// mmap.EXEC - The mapped memory is marked as executable.

// flag argument
// mmap.ANON - The mapped memory will not be backed by a file. If ANON is set in flags, f is ignored.
func read(file *os.File) ([]byte, error) {
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmapf.Unmap()
	result := make([]byte, len(mmapf))
	copy(result, mmapf)
	return result, nil
}

func readRange(file *os.File, startIndex, endIndex int) ([]byte, error) {
	if startIndex < 0 || endIndex < 0 || startIndex > endIndex {
		return nil, errors.New("indices invalid")
	}
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmapf.Unmap()
	if startIndex >= len(mmapf) || endIndex > len(mmapf) {
		return nil, errors.New("indices invalid")
	}
	result := make([]byte, endIndex-startIndex)
	copy(result, mmapf[startIndex:endIndex])
	return result, nil
}

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
