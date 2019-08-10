package mns

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"sort"
	"strings"
)

// sign 计算请求签名, 要求设置好请求 http 请求的 header 之后再调用.
func sign(httpMethod string, header http.Header, canonicalizedResource string, accessKeySecret string) string {
	h := hmac.New(sha1.New, []byte(accessKeySecret))
	bufw := bufio.NewWriterSize(h, 256)

	//    Signature = base64(hmac-sha1(VERB + "\n"
	//                + CONTENT-MD5 + "\n"
	//                + CONTENT-TYPE + "\n"
	//                + DATE + "\n"
	//                + CanonicalizedMNSHeaders
	//                + CanonicalizedResource))

	bufw.WriteString(httpMethod)
	bufw.WriteByte('\n')

	bufw.WriteString(header.Get("Content-Md5"))
	bufw.WriteByte('\n')

	bufw.WriteString(header.Get("Content-Type"))
	bufw.WriteByte('\n')

	bufw.WriteString(header.Get("Date"))
	bufw.WriteByte('\n')

	// 写入 CanonicalizedMNSHeaders
	var canonicalizedMNSHeaders canonicalizedMNSHeaders
	for k, vs := range header {
		if len(vs) == 0 {
			continue
		}
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-mns-") {
			v := vs[0]
			canonicalizedMNSHeaders = append(canonicalizedMNSHeaders, [2]string{k, v})
		}
	}
	if len(canonicalizedMNSHeaders) > 0 {
		sort.Sort(canonicalizedMNSHeaders) // 字典排序
		for i := 0; i < len(canonicalizedMNSHeaders); i++ {
			bufw.WriteString(canonicalizedMNSHeaders[i][0])
			bufw.WriteByte(':')
			bufw.WriteString(canonicalizedMNSHeaders[i][1])
			bufw.WriteByte('\n')
		}
	}

	// 写入 CanonicalizedResource
	bufw.WriteString(canonicalizedResource)

	bufw.Flush()
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

var _ sort.Interface = (canonicalizedMNSHeaders)(nil)

type canonicalizedMNSHeaders [][2]string // [][k,v]

func (p canonicalizedMNSHeaders) Len() int           { return len(p) }
func (p canonicalizedMNSHeaders) Less(i, j int) bool { return p[i][0] < p[j][0] }
func (p canonicalizedMNSHeaders) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
