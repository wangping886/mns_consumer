package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/wvanbergen/kafka/consumergroup"
	"strings"
)

const (
	OFFSETS_PROCESSING_TIMEOUT_SECONDS = 10
	OFFSETS_COMMIT_INTERVAL            = 10
	ZKRoot                             = ""
	GroupName                          = ""
	ProducerTopic                      = "topic"
)

var KafkaBrokerList = "127.0.0.1"
var ZKAddrs = []string{"127.0.0.1"}
var ConsumerFromTopics = []string{"topic"}

func main() {
	//init consumer
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = OFFSETS_PROCESSING_TIMEOUT_SECONDS
	config.Offsets.CommitInterval = OFFSETS_COMMIT_INTERVAL
	config.Zookeeper.Chroot = ZKRoot
	cg, err := consumergroup.JoinConsumerGroup(GroupName, ConsumerFromTopics, ZKAddrs, config)
	if err != nil {
		return err
	}

	//deal consumer error
	go func() {
		for err := range cg.Errors() {
			log.Println("consumer error(%v)", err)
		}
	}()

	//init producer
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Partitioner = sarama.NewHashPartitioner
	producer, err := sarama.NewSyncProducer(strings.Split(KafkaBrokerList, ","), producerConfig)
	if err != nil {
		log.Fatalln(err)
	}

	//convert
	go func() {
		for msg := range cg.Messages() {
			//todo 协程池消费
			log.Infof("topic:%s\n", (string)(msg.Topic))
			log.Infof("key:%s\n", (string)(msg.Key))
			log.Debugf("value:%s\n", (string)(msg.Value))
			log.Infof("partition:%d\n", msg.Partition)
			log.Infof("Msg.offset:%d\n", msg.Offset)

			msgByte, _ := json.Marshal(msg)
			//produce

			kafkaMsg := &sarama.ProducerMessage{Topic: "test-topic", Key: sarama.StringEncoder("key"), Value: sarama.StringEncoder(msgByte)}
			partition, offset, err := producer.SendMessage(kafkaMsg)
			var info string
			if err != nil {
				info = fmt.Sprintf("FAILED to send topic %s message: %s", ProducerTopic, string(msgByte), err)
			} else {
				info = fmt.Sprintf("message sent to topic %s partition %d at offset %d", ProducerTopic, partition, offset, string(msgByte))
			}
			log.Infof(info)

			//Zookeeper only keeps track of the latest offset that has been consumed, and assumes that everything before that offset has been handled.
			//It doesn't keep track of every individual message. //notice offset and retrive message from patition
			//ack
			cg.CommitUpto(msg)
			cg.FlushOffsets()
		}
	}()

	return nil
}
