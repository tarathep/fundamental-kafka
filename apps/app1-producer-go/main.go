package main

import (
	"fmt"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
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
	router := gin.Default()

	// This handler will match /user/john but will not match /user/ or /user
	router.GET("app1/produce/:message", func(c *gin.Context) {
		message := c.Param("message")
		Publish("A", message)
		c.String(http.StatusOK, "Published %s Topic A", message)
	})

	router.Run(":8081")

}
