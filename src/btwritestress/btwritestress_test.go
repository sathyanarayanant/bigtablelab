package main

import (
	"crypto/md5"
	"log"
	"testing"
)

func TestMd5Sum(t *testing.T) {
	bytes := md5.Sum([]byte("zycadatestdatazycdatatestmetric"))
	log.Printf("length of bytes: %v", len(bytes))




}
