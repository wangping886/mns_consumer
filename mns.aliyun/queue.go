package mns

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type QueueClient struct {
	QueueURL string // http://$AccountId.mns.<Region>.aliyuncs.com/queues/$QueueName

	AccessKeyId     string
	AccessKeySecret string

	HttpClient *http.Client // 默认为 http.DefaultClient
}

func (clt *QueueClient) getHttpClient() *http.Client {
	if httpClient := clt.HttpClient; httpClient != nil {
		return httpClient
	}
	return http.DefaultClient
}

type MessageToSend struct {
	XMLName      struct{} `xml:"Message"`
	MessageBody  []byte   `xml:"MessageBody"`            // 原始消息字节数组, UTF-8字符集
	DelaySeconds int      `xml:"DelaySeconds,omitempty"` // 0-604800秒（7天）范围内某个整数值，默认值为0
	Priority     int      `xml:"Priority,omitempty"`     // 取值范围1~16（其中1为最高优先级），默认优先级为8
}

// SendMessage 用于发送消息到指定的队列
//
//  QueueClient.SendMessage(msg) == QueueClient.SendMessage2(msg, true).
func (clt *QueueClient) SendMessage(msg *MessageToSend) (requestId string, messageId string, err error) {
	return clt.SendMessage2(msg, true)
}

// SendMessage2 用于发送消息到指定的队列.
//  msg:          待发送的消息
//  base64Encode: 为 true 时会对 msg.MessageBody 做 base64 编码, 然后再发送; 建议 msg.MessageBody 为可打印字符串时设置为 false.
func (clt *QueueClient) SendMessage2(msg *MessageToSend, base64Encode bool) (requestId string, messageId string, err error) {
	if msg == nil || len(msg.MessageBody) == 0 {
		err = errors.New("MessageBody must not be empty")
		return
	}

	_url, err := url.ParseRequestURI(clt.QueueURL + "/messages")
	if err != nil {
		return
	}

	if base64Encode {
		MessageBody := make([]byte, base64.StdEncoding.EncodedLen(len(msg.MessageBody)))
		base64.StdEncoding.Encode(MessageBody, msg.MessageBody)
		msg.MessageBody = MessageBody
	}

	body, err := xml.Marshal(msg)
	if err != nil {
		return
	}

	header := make(http.Header)
	header.Set("Content-Length", strconv.Itoa(len(body)))
	header.Set("Content-Type", __ContentTypeTextXML)
	header.Set("Content-Md5", contentMD5(body))
	header.Set("Date", formatDate(time.Now()))
	header.Set("Host", _url.Host)
	header.Set("X-Mns-Version", __ApiVersion)
	header.Set("Authorization", authorizationHeader(clt.AccessKeyId, sign(http.MethodPost, header, _url.RequestURI(), clt.AccessKeySecret)))

	httpReq := &http.Request{
		Method:        http.MethodPost,
		URL:           _url,
		Header:        header,
		Body:          ioutil.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Host:          _url.Host,
	}
	httpResp, err := clt.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	//	fmt.Println(httpResp.StatusCode)
	//	httpRespBody, _ := ioutil.ReadAll(httpResp.Body)
	//	httpResp.Body = ioutil.NopCloser(bytes.NewReader(httpRespBody))
	//	fmt.Println(string(httpRespBody))

	requestId = httpResp.Header.Get("X-Mns-Request-Id")

	switch {
	case httpResp.StatusCode/100 == 2:
		var result struct {
			XMLName        struct{} `xml:"Message"`
			MessageId      string   `xml:"MessageId"`
			MessageBodyMD5 string   `xml:"MessageBodyMD5"`
		}
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		if wantMessageBodyMD5 := messageBodyMD5(msg.MessageBody); strings.ToUpper(result.MessageBodyMD5) != wantMessageBodyMD5 {
			err = fmt.Errorf("MessageBodyMD5 mismatch, have %s, want %s", result.MessageBodyMD5, wantMessageBodyMD5)
			return
		}
		messageId = result.MessageId
		return
	default:
		var result ApiError
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		result.HttpStatusCode = httpResp.StatusCode
		if result.RequestId == "" {
			result.RequestId = requestId
		}
		err = &result
		return
	}
}

// BatchSendMessageResponseItem 是批量发送消息到指定的队列的返回结果, 每个 item 对应一个消息.
//
//  要么 ErrorCode, ErrorMessage 有效,
//  要么 MessageId, MessageBodyMD5 有效
type BatchSendMessageResponseItem struct {
	XMLName struct{} `xml:"Message"`

	ErrorCode    string `xml:"ErrorCode"`
	ErrorMessage string `xml:"ErrorMessage"`

	MessageId      string `xml:"MessageId"`
	MessageBodyMD5 string `xml:"MessageBodyMD5"`
}

// BatchSendMessage 用于批量发送消息到指定的队列
//
//  QueueClient.BatchSendMessage(msgs) == QueueClient.BatchSendMessage2(msgs, true)
func (clt *QueueClient) BatchSendMessage(msgs []MessageToSend) (requestId string, resp []BatchSendMessageResponseItem, err error) {
	return clt.BatchSendMessage2(msgs, true)
}

// BatchSendMessage2 用于批量发送消息到指定的队列
//  msgs:         待发送的消息列表
//  base64Encode: 为 true 时会对 msg.MessageBody 做 base64 编码, 然后再发送; 建议 msg.MessageBody 为可打印字符串时设置为 false.
func (clt *QueueClient) BatchSendMessage2(msgs []MessageToSend, base64Encode bool) (requestId string, resp []BatchSendMessageResponseItem, err error) {
	if len(msgs) < 1 || len(msgs) > 16 {
		err = errors.New("The length of msgs is invalid")
		return
	}
	for i := 0; i < len(msgs); i++ {
		if len(msgs[i].MessageBody) == 0 {
			err = errors.New("MessageBody must not be empty")
			return
		}
	}

	_url, err := url.ParseRequestURI(clt.QueueURL + "/messages")
	if err != nil {
		return
	}

	var req = struct {
		XMLName  struct{}        `xml:"Messages"`
		Messages []MessageToSend `xml:"Message,omitempty"`
	}{
		Messages: msgs,
	}

	if base64Encode {
		var MessageBody []byte
		for i := 0; i < len(req.Messages); i++ {
			MessageBody = make([]byte, base64.StdEncoding.EncodedLen(len(req.Messages[i].MessageBody)))
			base64.StdEncoding.Encode(MessageBody, req.Messages[i].MessageBody)
			req.Messages[i].MessageBody = MessageBody
		}
	}

	body, err := xml.Marshal(req)
	if err != nil {
		return
	}

	header := make(http.Header)
	header.Set("Content-Length", strconv.Itoa(len(body)))
	header.Set("Content-Type", __ContentTypeTextXML)
	header.Set("Content-Md5", contentMD5(body))
	header.Set("Date", formatDate(time.Now()))
	header.Set("Host", _url.Host)
	header.Set("X-Mns-Version", __ApiVersion)
	header.Set("Authorization", authorizationHeader(clt.AccessKeyId, sign(http.MethodPost, header, _url.RequestURI(), clt.AccessKeySecret)))

	httpReq := &http.Request{
		Method:        http.MethodPost,
		URL:           _url,
		Header:        header,
		Body:          ioutil.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Host:          _url.Host,
	}
	httpResp, err := clt.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	//	fmt.Println(httpResp.StatusCode)
	//	httpRespBody, _ := ioutil.ReadAll(httpResp.Body)
	//	httpResp.Body = ioutil.NopCloser(bytes.NewReader(httpRespBody))
	//	fmt.Println(string(httpRespBody))

	requestId = httpResp.Header.Get("X-Mns-Request-Id")

	switch httpResp.StatusCode {
	case 201, 500:
		var result struct {
			XMLName  struct{}                       `xml:"Messages"`
			Messages []BatchSendMessageResponseItem `xml:"Message"`
		}
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		if len(req.Messages) != len(result.Messages) {
			err = fmt.Errorf("result message count mismatch, have %d, want %d", len(result.Messages), len(req.Messages))
			return
		}
		for i := 0; i < len(result.Messages); i++ {
			if result.Messages[i].ErrorCode != "" {
				continue // 是错误信息, 直接跳过
			}
			// 检查 MD5
			wantMessageBodyMD5 := messageBodyMD5(req.Messages[i].MessageBody)
			if strings.ToUpper(result.Messages[i].MessageBodyMD5) != wantMessageBodyMD5 {
				err = fmt.Errorf("The %d'th MessageBodyMD5 mismatch, have %s, want %s", i, result.Messages[i].MessageBodyMD5, wantMessageBodyMD5)
				return
			}
		}
		resp = result.Messages
		return
	default:
		var result ApiError
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		result.HttpStatusCode = httpResp.StatusCode
		if result.RequestId == "" {
			result.RequestId = requestId
		}
		err = &result
		return
	}
}

type Message struct {
	XMLName          struct{} `xml:"Message"`
	MessageId        string   `xml:"MessageId"`
	ReceiptHandle    string   `xml:"ReceiptHandle"`
	MessageBody      []byte   `xml:"MessageBody"`
	MessageBodyMD5   string   `xml:"MessageBodyMD5"`
	EnqueueTime      int64    `xml:"EnqueueTime"`
	NextVisibleTime  int64    `xml:"NextVisibleTime"`
	FirstDequeueTime int64    `xml:"FirstDequeueTime"`
	DequeueCount     int      `xml:"DequeueCount"`
	Priority         int      `xml:"Priority"`
}

// ReceiveMessage 用于消费者消费队列中的消息
//
//  QueueClient.ReceiveMessage(waitSeconds) == QueueClient.ReceiveMessage2(waitSeconds, true)
func (clt *QueueClient) ReceiveMessage(waitSeconds int) (requestId string, msg *Message, err error) {
	return clt.ReceiveMessage2(waitSeconds, true)
}

func (clt *QueueClient) ReceiveMessage2(waitSeconds int, base64Decode bool) (requestId string, msg *Message, err error) {
	return clt.ReceiveMessage2Context(context.Background(), waitSeconds, base64Decode)
}

// ReceiveMessage2 用于消费者消费队列中的消息.
//  waitSeconds:  本次 ReceiveMessage2 请求最长的 Polling 等待时间，单位为秒, waitSeconds > 0 有效, 否则用队列默认值
//  base64Encode: 为 true 时会对接收到的 MessageBody 做 base64 解码; 注意要和 SendMessage2 的 base64Encode 保持一致.
func (clt *QueueClient) ReceiveMessage2Context(ctx context.Context, waitSeconds int, base64Decode bool) (requestId string, msg *Message, err error) {
	if waitSeconds < 0 || waitSeconds > 30 {
		waitSeconds = 30
	}

	rawurl := clt.QueueURL + "/messages"
	if waitSeconds > 0 {
		rawurl += "?waitseconds=" + strconv.Itoa(waitSeconds)
	}
	_url, err := url.ParseRequestURI(rawurl)
	if err != nil {
		return
	}

	header := make(http.Header)
	header.Set("Date", formatDate(time.Now()))
	header.Set("Host", _url.Host)
	header.Set("X-Mns-Version", __ApiVersion)
	header.Set("Authorization", authorizationHeader(clt.AccessKeyId, sign(http.MethodGet, header, _url.RequestURI(), clt.AccessKeySecret)))

	httpReq := &http.Request{
		Method: http.MethodGet,
		URL:    _url,
		Header: header,
		Host:   _url.Host,
	}
	httpReq = httpReq.WithContext(ctx)
	httpResp, err := clt.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	//	fmt.Println(httpResp.StatusCode)
	//	httpRespBody, _ := ioutil.ReadAll(httpResp.Body)
	//	httpResp.Body = ioutil.NopCloser(bytes.NewReader(httpRespBody))
	//	fmt.Println(string(httpRespBody))

	requestId = httpResp.Header.Get("X-Mns-Request-Id")

	switch {
	case httpResp.StatusCode/100 == 2:
		var result Message
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		if wantMessageBodyMD5 := messageBodyMD5(result.MessageBody); strings.ToUpper(result.MessageBodyMD5) != wantMessageBodyMD5 {
			err = fmt.Errorf("MessageBodyMD5 mismatch, have %s, want %s", result.MessageBodyMD5, wantMessageBodyMD5)
			return
		}
		if base64Decode && len(result.MessageBody) > 0 {
			MessageBody := make([]byte, base64.StdEncoding.DecodedLen(len(result.MessageBody)))
			n, err2 := base64.StdEncoding.Decode(MessageBody, result.MessageBody)
			if err2 != nil {
				err = fmt.Errorf("base64 decode MessageBody failed: %s", err.Error())
				return
			}
			result.MessageBody = MessageBody[:n]
		}
		msg = &result
		return
	default:
		var result ApiError
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		result.HttpStatusCode = httpResp.StatusCode
		if result.RequestId == "" {
			result.RequestId = requestId
		}
		err = &result
		return
	}
}

// BatchReceiveMessage 用于消费者批量消费队列的消息
//
//  QueueClient.BatchReceiveMessage(numOfMessages, waitSeconds) == QueueClient.BatchReceiveMessage2(numOfMessages, waitSeconds, true)
func (clt *QueueClient) BatchReceiveMessage(numOfMessages, waitSeconds int) (requestId string, msgs []Message, err error) {
	return clt.BatchReceiveMessage2(numOfMessages, waitSeconds, true)
}

func (clt *QueueClient) BatchReceiveMessage2(numOfMessages, waitSeconds int, base64Decode bool) (requestId string, msgs []Message, err error) {
	return clt.BatchReceiveMessage2Context(context.Background(), numOfMessages, waitSeconds, base64Decode)
}

// BatchReceiveMessage2 用于消费者批量消费队列的消息
//  numOfMessages: 本次 BatchReceiveMessage 最多获取的消息条数, 最多 16 条
//  waitSeconds:   本次 ReceiveMessage 请求最长的 Polling 等待时间，单位为秒, waitSeconds > 0 有效, 否则用队列默认值
//  base64Encode:  为 true 时会对接收到的 MessageBody 做 base64 解码; 注意要和 SendMessage2 的 base64Encode 保持一致.
func (clt *QueueClient) BatchReceiveMessage2Context(ctx context.Context, numOfMessages, waitSeconds int, base64Decode bool) (requestId string, msgs []Message, err error) {
	if numOfMessages < 1 || numOfMessages > 16 {
		numOfMessages = 16
	}
	if waitSeconds < 0 || waitSeconds > 30 {
		waitSeconds = 30
	}

	rawurl := clt.QueueURL + "/messages?numOfMessages=" + strconv.Itoa(numOfMessages)
	if waitSeconds > 0 {
		rawurl += "&waitseconds=" + strconv.Itoa(waitSeconds)
	}
	_url, err := url.ParseRequestURI(rawurl)
	if err != nil {
		return
	}

	header := make(http.Header)
	header.Set("Date", formatDate(time.Now()))
	header.Set("Host", _url.Host)
	header.Set("X-Mns-Version", __ApiVersion)
	header.Set("Authorization", authorizationHeader(clt.AccessKeyId, sign(http.MethodGet, header, _url.RequestURI(), clt.AccessKeySecret)))

	httpReq := &http.Request{
		Method: http.MethodGet,
		URL:    _url,
		Header: header,
		Host:   _url.Host,
	}
	httpReq = httpReq.WithContext(ctx)
	httpResp, err := clt.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	//	fmt.Println(httpResp.StatusCode)
	//	httpRespBody, _ := ioutil.ReadAll(httpResp.Body)
	//	httpResp.Body = ioutil.NopCloser(bytes.NewReader(httpRespBody))
	//	fmt.Println(string(httpRespBody))

	requestId = httpResp.Header.Get("X-Mns-Request-Id")

	switch {
	case httpResp.StatusCode/100 == 2:
		var result struct {
			XMLName  struct{}  `xml:"Messages"`
			Messages []Message `xml:"Message"`
		}
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		for i := 0; i < len(result.Messages); i++ {
			wantMessageBodyMD5 := messageBodyMD5(result.Messages[i].MessageBody)
			if strings.ToUpper(result.Messages[i].MessageBodyMD5) != wantMessageBodyMD5 {
				err = fmt.Errorf("The %d'th MessageBodyMD5 mismatch, have %s, want %s", i, result.Messages[i].MessageBodyMD5, wantMessageBodyMD5)
				return
			}
		}
		if base64Decode {
			for i := 0; i < len(result.Messages); i++ {
				if len(result.Messages[i].MessageBody) == 0 {
					continue
				}
				MessageBody := make([]byte, base64.StdEncoding.DecodedLen(len(result.Messages[i].MessageBody)))
				n, err3 := base64.StdEncoding.Decode(MessageBody, result.Messages[i].MessageBody)
				if err3 != nil {
					err = fmt.Errorf("base64 decode %d'th MessageBody failed: %s", i, err3.Error())
					return
				}
				result.Messages[i].MessageBody = MessageBody[:n]
			}
		}
		msgs = result.Messages
		return
	default:
		var result ApiError
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		result.HttpStatusCode = httpResp.StatusCode
		if result.RequestId == "" {
			result.RequestId = requestId
		}
		err = &result
		return
	}
}

type MessageFromPeek struct {
	XMLName          struct{} `xml:"Message"`
	MessageId        string   `xml:"MessageId"`
	MessageBody      []byte   `xml:"MessageBody"`
	MessageBodyMD5   string   `xml:"MessageBodyMD5"`
	EnqueueTime      int64    `xml:"EnqueueTime"`
	FirstDequeueTime int64    `xml:"FirstDequeueTime"`
	DequeueCount     int      `xml:"DequeueCount"`
	Priority         int      `xml:"Priority"`
}

// PeekMessage 用于消费者查看消息
//
//  QueueClient.PeekMessage() == QueueClient.PeekMessage2(true)
func (clt *QueueClient) PeekMessage() (requestId string, msg *MessageFromPeek, err error) {
	return clt.PeekMessage2(true)
}

func (clt *QueueClient) PeekMessage2(base64Decode bool) (requestId string, msg *MessageFromPeek, err error) {
	return clt.PeekMessage2Context(context.Background(), base64Decode)
}

// PeekMessage2 用于消费者查看消息
//  base64Encode:  为 true 时会对接收到的 MessageBody 做 base64 解码; 注意要和 SendMessage2 的 base64Encode 保持一致.
func (clt *QueueClient) PeekMessage2Context(ctx context.Context, base64Decode bool) (requestId string, msg *MessageFromPeek, err error) {
	_url, err := url.ParseRequestURI(clt.QueueURL + "/messages?peekonly=true")
	if err != nil {
		return
	}

	header := make(http.Header)
	header.Set("Date", formatDate(time.Now()))
	header.Set("Host", _url.Host)
	header.Set("X-Mns-Version", __ApiVersion)
	header.Set("Authorization", authorizationHeader(clt.AccessKeyId, sign(http.MethodGet, header, _url.RequestURI(), clt.AccessKeySecret)))

	httpReq := &http.Request{
		Method: http.MethodGet,
		URL:    _url,
		Header: header,
		Host:   _url.Host,
	}
	httpReq = httpReq.WithContext(ctx)
	httpResp, err := clt.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	//	fmt.Println(httpResp.StatusCode)
	//	httpRespBody, _ := ioutil.ReadAll(httpResp.Body)
	//	httpResp.Body = ioutil.NopCloser(bytes.NewReader(httpRespBody))
	//	fmt.Println(string(httpRespBody))

	requestId = httpResp.Header.Get("X-Mns-Request-Id")

	switch {
	case httpResp.StatusCode/100 == 2:
		var result MessageFromPeek
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		if wantMessageBodyMD5 := messageBodyMD5(result.MessageBody); strings.ToUpper(result.MessageBodyMD5) != wantMessageBodyMD5 {
			err = fmt.Errorf("MessageBodyMD5 mismatch, have %s, want %s", result.MessageBodyMD5, wantMessageBodyMD5)
			return
		}
		if base64Decode && len(result.MessageBody) > 0 {
			MessageBody := make([]byte, base64.StdEncoding.DecodedLen(len(result.MessageBody)))
			n, err2 := base64.StdEncoding.Decode(MessageBody, result.MessageBody)
			if err2 != nil {
				err = fmt.Errorf("base64 decode MessageBody failed: %s", err.Error())
				return
			}
			result.MessageBody = MessageBody[:n]
		}
		msg = &result
		return
	default:
		var result ApiError
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		result.HttpStatusCode = httpResp.StatusCode
		if result.RequestId == "" {
			result.RequestId = requestId
		}
		err = &result
		return
	}
}

// BatchPeekMessage 用于消费者批量查看消息
//
//  QueueClient.BatchPeekMessage(numOfMessages) == QueueClient.BatchPeekMessage2(numOfMessages, true)
func (clt *QueueClient) BatchPeekMessage(numOfMessages int) (requestId string, msgs []MessageFromPeek, err error) {
	return clt.BatchPeekMessage2(numOfMessages, true)
}

func (clt *QueueClient) BatchPeekMessage2(numOfMessages int, base64Decode bool) (requestId string, msgs []MessageFromPeek, err error) {
	return clt.BatchPeekMessage2Context(context.Background(), numOfMessages, base64Decode)
}

// BatchPeekMessage2 用于消费者批量查看消息
//  numOfMessages: 本次 BatchPeekMessage 最多查看消息条数, 最多 16 条
//  base64Encode:  为 true 时会对接收到的 MessageBody 做 base64 解码; 注意要和 SendMessage2 的 base64Encode 保持一致.
func (clt *QueueClient) BatchPeekMessage2Context(ctx context.Context, numOfMessages int, base64Decode bool) (requestId string, msgs []MessageFromPeek, err error) {
	if numOfMessages < 1 || numOfMessages > 16 {
		numOfMessages = 16
	}

	_url, err := url.ParseRequestURI(clt.QueueURL + "/messages?peekonly=true&numOfMessages=" + strconv.Itoa(numOfMessages))
	if err != nil {
		return
	}

	header := make(http.Header)
	header.Set("Date", formatDate(time.Now()))
	header.Set("Host", _url.Host)
	header.Set("X-Mns-Version", __ApiVersion)
	header.Set("Authorization", authorizationHeader(clt.AccessKeyId, sign(http.MethodGet, header, _url.RequestURI(), clt.AccessKeySecret)))

	httpReq := &http.Request{
		Method: http.MethodGet,
		URL:    _url,
		Header: header,
		Host:   _url.Host,
	}
	httpReq = httpReq.WithContext(ctx)
	httpResp, err := clt.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	//	fmt.Println(httpResp.StatusCode)
	//	httpRespBody, _ := ioutil.ReadAll(httpResp.Body)
	//	httpResp.Body = ioutil.NopCloser(bytes.NewReader(httpRespBody))
	//	fmt.Println(string(httpRespBody))

	requestId = httpResp.Header.Get("X-Mns-Request-Id")

	switch {
	case httpResp.StatusCode/100 == 2:
		var result struct {
			XMLName  struct{}          `xml:"Messages"`
			Messages []MessageFromPeek `xml:"Message"`
		}
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		for i := 0; i < len(result.Messages); i++ {
			wantMessageBodyMD5 := messageBodyMD5(result.Messages[i].MessageBody)
			if strings.ToUpper(result.Messages[i].MessageBodyMD5) != wantMessageBodyMD5 {
				err = fmt.Errorf("The %d'th MessageBodyMD5 mismatch, have %s, want %s", i, result.Messages[i].MessageBodyMD5, wantMessageBodyMD5)
				return
			}
		}
		if base64Decode {
			for i := 0; i < len(result.Messages); i++ {
				if len(result.Messages[i].MessageBody) == 0 {
					continue
				}
				MessageBody := make([]byte, base64.StdEncoding.DecodedLen(len(result.Messages[i].MessageBody)))
				n, err3 := base64.StdEncoding.Decode(MessageBody, result.Messages[i].MessageBody)
				if err3 != nil {
					err = fmt.Errorf("base64 decode %d'th MessageBody failed: %s", i, err3.Error())
					return
				}
				result.Messages[i].MessageBody = MessageBody[:n]
			}
		}
		msgs = result.Messages
		return
	default:
		var result ApiError
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		result.HttpStatusCode = httpResp.StatusCode
		if result.RequestId == "" {
			result.RequestId = requestId
		}
		err = &result
		return
	}
}

// DeleteMessage 用于删除已经被消费过的消息
func (clt *QueueClient) DeleteMessage(receiptHandle string) (requestId string, err error) {
	_url, err := url.ParseRequestURI(clt.QueueURL + "/messages?ReceiptHandle=" + url.QueryEscape(receiptHandle))
	if err != nil {
		return
	}

	header := make(http.Header)
	header.Set("Date", formatDate(time.Now()))
	header.Set("Host", _url.Host)
	header.Set("X-Mns-Version", __ApiVersion)
	header.Set("Authorization", authorizationHeader(clt.AccessKeyId, sign(http.MethodDelete, header, _url.RequestURI(), clt.AccessKeySecret)))

	httpReq := &http.Request{
		Method: http.MethodDelete,
		URL:    _url,
		Header: header,
		Host:   _url.Host,
	}
	httpResp, err := clt.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	//	fmt.Println(httpResp.StatusCode)
	//	httpRespBody, _ := ioutil.ReadAll(httpResp.Body)
	//	httpResp.Body = ioutil.NopCloser(bytes.NewReader(httpRespBody))
	//	fmt.Println(string(httpRespBody))

	requestId = httpResp.Header.Get("X-Mns-Request-Id")

	switch {
	case httpResp.StatusCode/100 == 2:
		return
	default:
		var result ApiError
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		result.HttpStatusCode = httpResp.StatusCode
		if result.RequestId == "" {
			result.RequestId = requestId
		}
		err = &result
		return
	}
}

type BatchDeleteMessageErrorItem struct {
	XMLName       struct{} `xml:"Error"`
	ErrorCode     string   `xml:"ErrorCode"`
	ErrorMessage  string   `xml:"ErrorMessage"`
	ReceiptHandle string   `xml:"ReceiptHandle"`
}

// BatchDeleteMessage 批量删除队列多条消息，最多可以删除16条消息
//  receiptHandles: 需要删除的消息的 ReceiptHandle 列表
//  requestId:      本次请求的 requestId
//  Errors:         删除出错(失败)的消息和错误信息
//  err:            api 请求错误信息
func (clt *QueueClient) BatchDeleteMessage(receiptHandles []string) (requestId string, Errors []BatchDeleteMessageErrorItem, err error) {
	if len(receiptHandles) < 1 || len(receiptHandles) > 16 {
		err = errors.New("the length of receiptHandles is invalid")
		return
	}

	_url, err := url.ParseRequestURI(clt.QueueURL + "/messages")
	if err != nil {
		return
	}

	var req = struct {
		XMLName        struct{} `xml:"ReceiptHandles"`
		ReceiptHandles []string `xml:"ReceiptHandle,omitempty"`
	}{
		ReceiptHandles: receiptHandles,
	}

	body, err := xml.Marshal(req)
	if err != nil {
		return
	}

	header := make(http.Header)
	header.Set("Content-Length", strconv.Itoa(len(body)))
	header.Set("Content-Type", __ContentTypeTextXML)
	header.Set("Content-Md5", contentMD5(body))
	header.Set("Date", formatDate(time.Now()))
	header.Set("Host", _url.Host)
	header.Set("X-Mns-Version", __ApiVersion)
	header.Set("Authorization", authorizationHeader(clt.AccessKeyId, sign(http.MethodDelete, header, _url.RequestURI(), clt.AccessKeySecret)))

	httpReq := &http.Request{
		Method:        http.MethodDelete,
		URL:           _url,
		Header:        header,
		Body:          ioutil.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Host:          _url.Host,
	}
	httpResp, err := clt.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	//	fmt.Println(httpResp.StatusCode)
	//	httpRespBody, _ := ioutil.ReadAll(httpResp.Body)
	//	httpResp.Body = ioutil.NopCloser(bytes.NewReader(httpRespBody))
	//	fmt.Println(string(httpRespBody))

	requestId = httpResp.Header.Get("X-Mns-Request-Id")

	switch {
	case httpResp.StatusCode/100 == 2:
		return
	case httpResp.StatusCode == 404:
		body, err2 := ioutil.ReadAll(httpResp.Body)
		if err2 != nil {
			err = err2
			return
		}
		if bytes.Contains(body, []byte(`<ReceiptHandle>`)) {
			var result struct {
				XMLName struct{}                      `xml:"Errors"`
				Errors  []BatchDeleteMessageErrorItem `xml:"Error"`
			}
			if err = xml.Unmarshal(body, &result); err != nil {
				return
			}
			Errors = result.Errors
			return
		} else {
			var result ApiError
			if err = xml.Unmarshal(body, &result); err != nil {
				return
			}
			result.HttpStatusCode = httpResp.StatusCode
			if result.RequestId == "" {
				result.RequestId = requestId
			}
			err = &result
			return
		}
	default:
		var result ApiError
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		result.HttpStatusCode = httpResp.StatusCode
		if result.RequestId == "" {
			result.RequestId = requestId
		}
		err = &result
		return
	}
}

type ChangeMessageVisibilityResponse struct {
	XMLName         struct{} `xml:"ChangeVisibility"`
	ReceiptHandle   string   `xml:"ReceiptHandle"`
	NextVisibleTime int64    `xml:"NextVisibleTime"`
}

// ChangeMessageVisibility 用于修改被消费过并且还处于的 Inactive 的消息到下次可被消费的时间，
// 成功修改消息的 VisibilityTimeout 后，返回新的 ReceiptHandle
func (clt *QueueClient) ChangeMessageVisibility(receiptHandle string, visibilityTimeout int) (requestId string, resp *ChangeMessageVisibilityResponse, err error) {
	rawurl := clt.QueueURL + "/messages?receiptHandle=" + url.QueryEscape(receiptHandle) + "&visibilityTimeout=" + strconv.Itoa(visibilityTimeout)
	_url, err := url.ParseRequestURI(rawurl)
	if err != nil {
		return
	}

	header := make(http.Header)
	header.Set("Date", formatDate(time.Now()))
	header.Set("Host", _url.Host)
	header.Set("X-Mns-Version", __ApiVersion)
	header.Set("Authorization", authorizationHeader(clt.AccessKeyId, sign(http.MethodPut, header, _url.RequestURI(), clt.AccessKeySecret)))

	httpReq := &http.Request{
		Method: http.MethodPut,
		URL:    _url,
		Header: header,
		Host:   _url.Host,
	}
	httpResp, err := clt.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	defer httpResp.Body.Close()

	//	fmt.Println(httpResp.StatusCode)
	//	httpRespBody, _ := ioutil.ReadAll(httpResp.Body)
	//	httpResp.Body = ioutil.NopCloser(bytes.NewReader(httpRespBody))
	//	fmt.Println(string(httpRespBody))

	requestId = httpResp.Header.Get("X-Mns-Request-Id")

	switch {
	case httpResp.StatusCode/100 == 2:
		var result ChangeMessageVisibilityResponse
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		resp = &result
		return
	default:
		var result ApiError
		if err = xml.NewDecoder(httpResp.Body).Decode(&result); err != nil {
			return
		}
		result.HttpStatusCode = httpResp.StatusCode
		if result.RequestId == "" {
			result.RequestId = requestId
		}
		err = &result
		return
	}
}
