package mns

import (
	"encoding/xml"
)

var _ error = (*ApiError)(nil)

// ApiError 是 MNS 的错误响应.
type ApiError struct {
	XMLName        struct{} `xml:"Error" json:"-"`
	HttpStatusCode int      `xml:"HttpStatusCode" json:"HttpStatusCode"` // HTTP 状态码
	Code           string   `xml:"Code" json:"Code"`                     // MNS 返回给用户的错误码。
	Message        string   `xml:"Message" json:"Message"`               // MNS 给出的详细错误信息。
	RequestId      string   `xml:"RequestId" json:"RequestId"`           // 用于唯一标识该次请求的编号；当你无法解决问题时，可以提供这个 RequestId 寻求 MNS 支持工程师的帮助。
	HostId         string   `xml:"HostId" json:"HostId"`                 // 用于标识访问的 MNS 服务的地域。
}

func (err *ApiError) Error() string {
	b, _ := xml.Marshal(err)
	return string(b)
}
