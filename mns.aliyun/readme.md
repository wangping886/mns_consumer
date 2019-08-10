### NOTE
对于所有的api操作, 返回的错误最好做类型断言, 然后根据文档做对应的逻辑处理
```Go
if v, ok := err.(*mns.ApiError); ok {
	// TODO: 逻辑操作
	_ = v
}
```

### 队列消息的发送、接收和删除
```Go
package main

import (
	"fmt"

	"code.qschou.com/golang/go_sdk_mq/mns.aliyun.v20150606"
)

func main() {
	clt := mns.QueueClient{
		QueueURL:        "xxxx",
		AccessKeyId:     "xxxx",
		AccessKeySecret: "xxxx",
	}

	msg := mns.MessageToSend{
		MessageBody: []byte("test_msg"),
	}
	fmt.Println(clt.SendMessage(&msg))

	_, msg3, err := clt.PeekMessage()
	if err != nil {
		if v, ok := err.(*mns.ApiError); ok {
			// TODO: 逻辑操作
			_ = v
		}
		fmt.Println(err)
		return
	}
	fmt.Println(string(msg3.MessageBody))

	_, msg2, err := clt.ReceiveMessage(0)
	if err != nil {
		if v, ok := err.(*mns.ApiError); ok {
			// TODO: 逻辑操作
			_ = v
		}
		fmt.Println(err)
		return
	}
	fmt.Println(string(msg2.MessageBody))

	fmt.Println(clt.DeleteMessage(msg2.ReceiptHandle))
}
```

### 批量消息的发送、接收和删除
```Go
package main

import (
	"fmt"

	"code.qschou.com/golang/go_sdk_mq/mns.aliyun.v20150606"
)

func main() {
	clt := mns.QueueClient{
		QueueURL:        "xxxx",
		AccessKeyId:     "xxxx",
		AccessKeySecret: "xxxx",
	}

	msgs := []mns.MessageToSend{
		{
			MessageBody: []byte("test_msg1"),
		},
		{
			MessageBody: []byte("test_msg2"),
		},
	}
	_, resp, err := clt.BatchSendMessage(msgs)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < len(resp); i++ {
		if resp[i].ErrorCode != "" {
			fmt.Println(i, resp[i].ErrorCode, resp[i].ErrorMessage)
		}
	}

	_, msgs2, err := clt.BatchPeekMessage(2)
	if err != nil {
		if v, ok := err.(*mns.ApiError); ok {
			// TODO: 逻辑操作
			_ = v
		}
		fmt.Println(err)
		return
	}
	for i := 0; i < len(msgs2); i++ {
		fmt.Println(string(msgs2[i].MessageBody))
	}

	_, msgs3, err := clt.BatchReceiveMessage(2, 0)
	if err != nil {
		if v, ok := err.(*mns.ApiError); ok {
			// TODO: 逻辑操作
			_ = v
		}
		fmt.Println(err)
		return
	}
	for i := 0; i < len(msgs3); i++ {
		fmt.Println(string(msgs3[i].MessageBody))
	}

	receiptHandles := make([]string, len(msgs3))
	for i := 0; i < len(receiptHandles); i++ {
		receiptHandles[i] = msgs3[i].ReceiptHandle
	}
	fmt.Println(clt.BatchDeleteMessage(receiptHandles))
}
```