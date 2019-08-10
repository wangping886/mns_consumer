package mns

// Notification XML 完整格式
type Notification struct {
	XMLName          struct{} `xml:"Notification"`
	TopicOwner       string   `xml:"TopicOwner"`
	TopicName        string   `xml:"TopicName"`
	Subscriber       string   `xml:"Subscriber"`
	SubscriptionName string   `xml:"SubscriptionName"`
	MessageId        string   `xml:"MessageId"`
	Message          []byte   `xml:"Message"`
	MessageMD5       string   `xml:"MessageMD5"`
	MessageTag       string   `xml:"MessageTag"`
	PublishTime      int64    `xml:"PublishTime"`
}
