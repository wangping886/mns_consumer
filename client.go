package mq_consumer

import (
	"net"
	"net/http"
	"time"

	"github.com/wangping886/mns_consumer/mns.aliyun.v20150606"
)

const (
	MNSEndPoint  = "xxxx"
	MNSAccessId  = "xxxx"
	MNSAccessKey = "xxxx"
)

var __aliyunMnsQueueHttpClient *http.Client
var __aliyunMnsTopicHttpClient *http.Client

//超时时间改为5秒，这个是因为，获取队列消息的时候会主动block 3秒
func init() {
	//消费queue的使用
	__aliyunMnsQueueHttpClient = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			Dial: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   20,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 2 * time.Second,
		},
		Timeout: 25 * time.Second,
	}
	//写入topic的时候使用
	__aliyunMnsTopicHttpClient = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			Dial: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   20,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 3 * time.Second,
		},
		Timeout: 3 * time.Second,
	}
}

// 设置Queue client
func SetQueue(queue string) *mns.QueueClient {
	Result := &mns.QueueClient{
		QueueURL:        MNSEndPoint + "/queues/" + queue,
		AccessKeyId:     MNSAccessId,
		AccessKeySecret: MNSAccessKey,
		HttpClient:      __aliyunMnsQueueHttpClient,
	}
	return Result
}

// 设置Topic client
func SetTopic(topic string) *mns.TopicClient {
	Result := &mns.TopicClient{
		TopicURL:        MNSEndPoint + "/topics/" + topic,
		AccessKeyId:     MNSAccessId,
		AccessKeySecret: MNSAccessKey,
		HttpClient:      __aliyunMnsTopicHttpClient,
	}
	return Result
}
