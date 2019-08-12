package main

import (
	"context"
	"github.com/wangping886/mns_consumer/consumer"
	"github.com/wangping886/mns_consumer/mns.aliyun"
	"signal"
	"log"
	"os"
	"syscall"
)

func main() {

	c := consumer.NewConsumer("your-queue-name", ProcessMessage, consumer.WithLimitSize(20), consumer.WithChanSize(32))
	c.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sig := <-sigCh

	log.Println("accept signal", sig.String())

	c.Stop()
}
func ProcessMessage(c *consumer.Consumer, mnsMsg mns.Message) {
	defer func() {
		<-c.LimitChan
	}()
	defer c.Delete(context.Background(), mnsMsg)

	log.Println("consumer get a msg", string(mnsMsg.MessageBody))
}
