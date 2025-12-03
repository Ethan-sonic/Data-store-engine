package Methods

import (
	"encoding/binary"
	"fmt"
	"os"
)

func GetUint64(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}

// AppendKey Vraca keySize i Key kao niz bajtova
func AppendKey(bytes *[]byte, value []byte) {
	*bytes = append(*bytes, value[13:17]...) // Key Size
	keySize := binary.LittleEndian.Uint32(value[13:17])
	*bytes = append(*bytes, value[21:21+keySize]...) // Key
}

// AppendNum Konvertuje broj u niz bajtova i appenduje ga na prosledjeni niz bajtova
func AppendNum(bytes *[]byte, number uint32) {
	numBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(numBytes, number)
	*bytes = append(*bytes, numBytes...)
}

// AppendKeyOffset Na zadati niz bajtova appenduje velicinu, vresnost kljuca, i offset za odredjenu strukturu
func AppendKeyOffset(bytes *[]byte, value []byte, offset uint32) {
	AppendKey(bytes, value)
	AppendNum(bytes, offset)
}


// ReadUint32  Procita i vraca vrendost tipa uint32 iz fajla
func ReadUint32(file *os.File) (uint32, error) {
	numberBytes := make([]byte, 4)
	_, err := file.Read(numberBytes)
	if err != nil {
		return 0, err
	}
	number := binary.LittleEndian.Uint32(numberBytes)
	return number, nil
}

// ReadStringBytes Cita string iz fajla. Prvo procita velicinu i na osnovu te velicine procita kompletan string
func ReadStringBytes(file *os.File) ([]byte, error) {
	strSize, err := ReadUint32(file)
	if err != nil {
		return nil, err
	}
	strBytes := make([]byte, strSize)
	_, err = file.Read(strBytes)
	if err != nil {
		return nil, err
	}
	return strBytes, nil
}

// ReadString Vraca vrednost bas tipa string
func ReadString(file *os.File) (string, error) {
	strBytes, err := ReadStringBytes(file)
	return string(strBytes), err
}

// OpenSSTFile Otvara segment SSTable-a
func OpenSSTFile(sstype string, lvl uint8, i uint8) *os.File {
	path := fmt.Sprintf("data/sstable/%d-usertable-%d-%s", lvl, i, sstype)
	file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0777)
	if err != nil { fmt.Println(err) }
	return file
}

// Rec1AfterRec2 Proverava da li je rec1 kasnije napravljen u odnosu na rec2
func Rec1AfterRec2(rec1, rec2 []byte) bool {
	timestamp1 := GetUint64(rec1[4:12])
	timestamp2 := GetUint64(rec2[4:12])
	return timestamp1 > timestamp2
}

// NextKey Seek-uje offset u fajlu da bi mogao da procita naredni kljuc
func NextKey(file *os.File) (string, error) {
	_, err := file.Seek(4, 1)
	if err != nil { return "", err }
	key, err := ReadString(file)
	return key, err
}
