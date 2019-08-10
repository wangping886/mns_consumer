package mns

import (
	"bytes"
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

type TopicClient struct {
	TopicURL string // http://$AccountId.mns.<Region>.aliyuncs.com/topics/$TopicName

	AccessKeyId     string
	AccessKeySecret string

	HttpClient *http.Client // 默认为 http.DefaultClient
}

func (clt *TopicClient) getHttpClient() *http.Client {
	if httpClient := clt.HttpClient; httpClient != nil {
		return httpClient
	}
	return http.DefaultClient
}

type MessageToPublish struct {
	XMLName           struct{}                    `xml:"Message"`
	MessageBody       []byte                      `xml:"MessageBody"`
	MessageTag        string                      `xml:"MessageTag,omitempty"`
	MessageAttributes *MessageToPublishAttributes `xml:"MessageAttributes,omitempty"`
}

type MessageToPublishAttributes struct {
	DirectMail []byte `xml:"DirectMail,omitempty"`
}

// PublishMessage 用于发布者向指定的主题发布消息, 消息发布到主题后随即会被推送给 Endpoint 消费.
//
//  TopicClient.PublishMessage(msg) == TopicClient.PublishMessage2(msg, true)
func (clt *TopicClient) PublishMessage(msg *MessageToPublish) (requestId string, messageId string, err error) {
	return clt.PublishMessage2(msg, true)
}

// PublishMessage2 用于发布者向指定的主题发布消息, 消息发布到主题后随即会被推送给 Endpoint 消费.
//  msg:          待发送的消息
//  base64Encode: 为 true 时会对 msg.MessageBody 做 base64 编码, 然后再发送; 建议 msg.MessageBody 为可打印字符串时设置为 false.
func (clt *TopicClient) PublishMessage2(msg *MessageToPublish, base64Encode bool) (requestId string, messageId string, err error) {
	if msg == nil || len(msg.MessageBody) == 0 {
		err = errors.New("MessageBody must not be empty")
		return
	}

	_url, err := url.ParseRequestURI(clt.TopicURL + "/messages")
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
