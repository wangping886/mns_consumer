package mns

import (
	"bytes"
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"

	"github.com/chanxuehong/lrucache"
)

var __certCache = lrucache.New(100) // 缓存已经下载并解析过的 X509 证书

// VerifyNotification 验证消息通知来源
//  req:  消息通知的回调请求的 http.Request 结构
//  body: 消息通知的回调请求的 http body
func VerifyNotification(req *http.Request, body []byte) (ok bool, err error) {
	return verifyNotification(req, body, true)
}

// verifyNotification 验证消息通知来源
//  req:              消息通知的回调请求的 http.Request 结构
//  body:             消息通知的回调请求的 http body
//  verifyContentMD5: 为 true 时验证 Content-MD5 请求头
func verifyNotification(req *http.Request, body []byte, verifyContentMD5 bool) (bool, error) {
	// 验证 Content-MD5 header, 如果存在的话
	if verifyContentMD5 {
		if haveContentMD5 := req.Header.Get("Content-Md5"); haveContentMD5 != "" {
			if wantContentMD5 := contentMD5(body); haveContentMD5 != wantContentMD5 {
				return false, fmt.Errorf("Content-MD5 mismatch, have %s, want %s", haveContentMD5, wantContentMD5)
			}
		}
	}

	// 获取 Authorization header
	Authorization := req.Header.Get("Authorization")
	if Authorization == "" {
		return false, errors.New("Authorization header not found")
	}

	// 获取 x-mns-signing-cert-url header
	base64CertURL := req.Header.Get("X-Mns-Signing-Cert-Url")
	if base64CertURL == "" {
		return false, errors.New("x-mns-signing-cert-url header not found")
	}
	certURLBytes, err := base64.StdEncoding.DecodeString(base64CertURL)
	if err != nil {
		return false, fmt.Errorf("invalid x-mns-signing-cert-url header: %s", base64CertURL)
	}
	certURL := string(certURLBytes)
	if !strings.HasPrefix(certURL, "http://mnstest.oss-cn-hangzhou.aliyuncs.com/") {
		return false, fmt.Errorf("invalid x-mns-signing-cert-url header: %s", base64CertURL)
	}

	// 下载并解析 x-mns-signing-cert-url 证书
	pubKey, err := getRSAPublicKey(certURL)
	if err != nil {
		return false, err
	}

	// 计算待签名字符串
	signStr := getSignStr(req)

	// 验证签名
	return rsaVerify(signStr, pubKey, Authorization)
}

func getRSAPublicKey(certURL string) (key *rsa.PublicKey, err error) {
	cachedValue, err := __certCache.Get(certURL)
	switch err {
	case nil:
		if key, ok := cachedValue.(*rsa.PublicKey); ok {
			return key, nil
		}
		fallthrough
	case lrucache.ErrNotFound:
		certBytes, err := downloadCertificate(certURL)
		if err != nil {
			return nil, err
		}
		pubKey, err := parseRSAPublicKey(certBytes)
		if err != nil {
			return nil, err
		}
		__certCache.Set(certURL, pubKey)
		return pubKey, nil
	default:
		return nil, err
	}
}

// downloadCertificate 下载 X509 证书
func downloadCertificate(url string) (data []byte, err error) {
	resp, err := http.DefaultClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

// parseRSAPublicKey 从证书里解析出公钥
func parseRSAPublicKey(pemBytes []byte) (key *rsa.PublicKey, err error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("incorrect pem bytes")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("it's not an RSA public key")
	}
	return
}

// getSignStr 计算待签名字符串
func getSignStr(r *http.Request) []byte {
	//    VERB + "\n"
	//    + CONTENT-MD5 + "\n"
	//    + CONTENT-TYPE + "\n"
	//    + DATE + "\n"
	//    + CanonicalizedMNSHeaders
	//    + CanonicalizedResource

	var buf bytes.Buffer

	buf.WriteString(r.Method)
	buf.WriteByte('\n')
	buf.WriteString(r.Header.Get("Content-Md5"))
	buf.WriteByte('\n')
	buf.WriteString(r.Header.Get("Content-Type"))
	buf.WriteByte('\n')
	buf.WriteString(r.Header.Get("Date"))
	buf.WriteByte('\n')

	// 写入 CanonicalizedMNSHeaders
	var canonicalizedMNSHeaders canonicalizedMNSHeaders
	for k, vs := range r.Header {
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
			buf.WriteString(canonicalizedMNSHeaders[i][0])
			buf.WriteByte(':')
			buf.WriteString(canonicalizedMNSHeaders[i][1])
			buf.WriteByte('\n')
		}
	}

	// 写入 CanonicalizedResource
	buf.WriteString(r.RequestURI)
	return buf.Bytes()
}

// rsaVerify 验证签名
//  data:      待签名的字节数组
//  publicKey: 公钥
//  sign:      待验证的签名, base64 编码
func rsaVerify(data []byte, publicKey *rsa.PublicKey, sign string) (ok bool, err error) {
	sig, err := base64.StdEncoding.DecodeString(sign)
	if err != nil {
		return false, err
	}
	digest := sha1.Sum(data)
	if err = rsa.VerifyPKCS1v15(publicKey, crypto.SHA1, digest[:], sig); err != nil {
		return false, nil
	}
	return true, nil
}
