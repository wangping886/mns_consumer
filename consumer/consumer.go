package consumer

import (
	"context"
	"encoding/xml"
	"net"
	"time"

	"gopkg.in/tomb.v1"

	"github.com/wangping886/mns_consumer/mns.aliyun"
	"log"

	"github.com/wangping886/mns_consumer/util"
)

const defaultTimeoutMaxRetry = 5

type Handler func(*Consumer, mns.Message)

type queueMsg struct {
	c      *Consumer
	mnsMsg mns.Message
}

type Consumer struct {
	client          *mns.QueueClient
	hanlder         Handler
	t               tomb.Tomb
	timeoutMaxRetry int
	queSize         int
	limitSize       int
	queMsgChan      chan queueMsg
	LimitChan       chan bool // 并发数
	w               util.WaitGroupWrapper
	serveDone       chan struct{}
}

type option func(c *Consumer)

func NewConsumer(queName string, handler Handler, options ...option) *Consumer {
	c := &Consumer{
		client:          SetQueue(queName),
		hanlder:         handler,
		timeoutMaxRetry: defaultTimeoutMaxRetry,
		serveDone:       make(chan struct{}),
	}

	for _, o := range options {
		o(c)
	}

	c.queMsgChan = make(chan queueMsg, c.queSize)
	c.LimitChan = make(chan bool, c.limitSize)
	return c
}

func WithTimeoutRetry(retry int) option {
	return func(c *Consumer) {
		c.timeoutMaxRetry = retry
	}
}

func WithChanSize(size int) option {
	return func(c *Consumer) {
		c.queSize = size
	}
}

func WithLimitSize(size int) option {
	return func(c *Consumer) {
		c.limitSize = size
	}
}

func (c *Consumer) Start() {
	c.w.Wrap(c.startQueueWorker)
	c.w.Wrap(c.serve)

	go func() {
		c.w.Wait()
		c.t.Done()
	}()
}

func (c *Consumer) serve() {
	var (
		i    int
		msgs []mns.Message
		err  error
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-c.t.Dying()
		cancel()
	}()

	for {
		for i = 0; i < c.timeoutMaxRetry; i++ {
			_, msgs, err = c.client.BatchReceiveMessage2Context(ctx, 16, 20, false) // 每次最多可以取 16 个消息
			if err == nil {
				break
			}
			if timeoutErr(err) || mnsMsgNotFound(err) {
				continue // 连接超时重试
			} else {
				log.Println("consumerServe", "receiveMessageFail", "err", err)
				break
			}
		}

		if err != nil {
			select {
			case <-ctx.Done():
				//context.DeadlineExceeded, context.Canceled
				log.Println("msg", ctx.Err(), "method", "consumer.Serve", "func", "ctx.Done")

				goto DONE
			default:
			}
		}

		if err != nil {
			if mnsMsgNotFound(err) {
				log.Println("errmsg", err, "method", "consumer.Serve", "func", "getMsg")

				continue
			}
			time.Sleep(time.Second)
			continue
		}

		for _, msg := range msgs {
			c.LimitChan <- true
			c.queMsgChan <- queueMsg{
				c:      c,
				mnsMsg: msg,
			}
		}
	}
DONE:
	close(c.serveDone)
	log.Println("msg", "serve.done", "method", "consumer.Serve")

}

func (c *Consumer) startQueueWorker() {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	go func() {
		<-c.serveDone
		close(c.queMsgChan)
	}()

	for msg := range c.queMsgChan {
		<-tick.C
		go c.hanlder(msg.c, msg.mnsMsg)
	}
	log.Println("msg", "worker.done", "method", "startQueueWorker")

}

func (c *Consumer) Stop() {
	c.t.Kill(nil)
	c.t.Wait()
	log.Println("msg", "consumer.done", "method", "Stop")

}

func (c *Consumer) Delete(ctx context.Context, msg mns.Message) {
	log.Println("method", "mns.Delete", "msgID", msg.MessageId)

	var err error

	for i := 0; i < c.timeoutMaxRetry; i++ {
		_, err = c.client.DeleteMessage(msg.ReceiptHandle)
		if err == nil {
			break
		}
		if timeoutErr(err) {
			continue
		} else {
			break
		}
	}

	if err != nil {
		b, _ := xml.Marshal(&msg)
		log.Println("msgstr", string(b))
	}
}

func timeoutErr(err error) bool {
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	return false
}

func mnsMsgNotFound(err error) bool {
	if mnsApiError, ok := err.(*mns.ApiError); ok && mnsApiError.HttpStatusCode == 404 && mnsApiError.Code == "MessageNotExist" {
		return true
	}
	return false
}
