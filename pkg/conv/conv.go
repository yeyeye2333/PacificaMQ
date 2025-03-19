package conv

import (
	"bytes"
	"encoding/binary"
)

func Int32ToBytes(i int32) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.NativeEndian, i)
	return bytesBuffer.Bytes()
}

func BytesToInt32(b []byte) int32 {
	bytesBuffer := bytes.NewBuffer(b)
	var i int32
	binary.Read(bytesBuffer, binary.NativeEndian, &i)
	return i
}
