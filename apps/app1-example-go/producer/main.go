package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

var brokers = []string{"127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"}

func newProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}

func prepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(message),
	}
	return msg
}

//Publish is func for produce message to kafka
func Publish(topic string, message string) {
	producer, err := newProducer()
	if err != nil {
		fmt.Println("Could not create producer: ", err)
	}

	msg := prepareMessage(topic, message)
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(partition, offset)
		// fmt.Fprintf("Message was saved to partion:" + partition + ".\nMessage offset is" + offset)
	}

}

func main() {
	Publish("topicA", `{"topic":"topicA","message":"hello"}`)
}
