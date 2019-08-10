package mns

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"time"
)

// authorizationHeader 返回 Authorization http header.
func authorizationHeader(accessKeyId, signature string) string {
	return "MNS " + accessKeyId + ":" + signature
}

// https://tools.ietf.org/html/rfc1864
func contentMD5(b []byte) string {
	sum := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// "Thu, 17 Mar 2012 18:49:58 GMT"
func formatDate(t time.Time) string {
	return t.UTC().Format(__DateLayout)
}

// messageBodyMD5 返回消息体的 md5.
func messageBodyMD5(b []byte) string {
	sum := md5.Sum(b)
	var hexSum [md5.Size * 2]byte
	hex.Encode(hexSum[:], sum[:])
	return string(bytes.ToUpper(hexSum[:]))
}
