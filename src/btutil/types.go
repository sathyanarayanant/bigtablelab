package btutil

import (
	"fmt"
	"crypto/md5"
	"strconv"
	"bytes"
	"encoding/binary"
	"log"
	"strings"
	"errors"
)

type RowKey string

func (r RowKey) Epochsec() (uint32, error) {
	split := strings.Split(string(r), "_")

	if len(split) != 2 {
		err := errors.New(fmt.Sprintf("invalid row key [%v]. should have 2 tokens after splitting by _", r))
		log.Printf("err [%v]", err)
		return 0, err
	}

	epochsec, err := strconv.Atoi(split[1])
	if err != nil {
		log.Printf("%v", err)
		return 0, err
	}

	return uint32(epochsec), nil
}


type KeyValueEpochsec struct {
	Key      string
	Value    float64
	Epochsec uint32
}

func (k *KeyValueEpochsec) BTRowKey() RowKey {

	return RowKey(k.BTRowKeyStr())
}

func (k *KeyValueEpochsec) BTRowKeyStr() string {

	md5Sum := fmt.Sprintf("%x", md5.Sum([]byte(k.Key)))
	s := md5Sum + "_" + strconv.Itoa(int(k.Epochsec))
	return s
}

func (k *KeyValueEpochsec) ValueByteArray() []byte {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, k.Value)
	if err != nil {
		log.Fatalf("cannot convert float [%v] to byte array, err [%v]", k.Value, err)
	}

	return buffer.Bytes()
}
