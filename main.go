package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
)

// doConsumer means read it, dude
func doConsumer(topic string, broker string) {
	fmt.Println("This will read")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           "EdwinReadGroup",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	var topics []string

	topics = make([]string, 1, 1)

	topics[0] = topic

	err = c.SubscribeTopics(topics, nil)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()

}

// doProducer means write it, dude
func doProducer(topic string, broker string) {
	configMap := kafka.ConfigMap{"bootstrap.servers": broker,
		//"batch.size":     16384,
		"linger.ms": 1,
		// "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
		//		"value.serializer": "org.apache.kafka.common.serialization.StringSerializer",
		//"acks":      1,
		"client.id": "edwin-golang"}

	fmt.Printf("the map is: %+v\n", configMap)

	p, err := kafka.NewProducer(&configMap)

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	// var ran UUID

	for i := 0; i < 100; i++ {

		ran, uerr := uuid.NewV4()

		if uerr != nil {
			panic(uerr)
		}

		value := "Hello Go! " + strconv.Itoa(i) + " " + ran.String()

		fmt.Printf("\tWriting: '%s'\n", value)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}, deliveryChan)

		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}

	} // for i := 0; i < 100; i++ {

	close(deliveryChan)

}

func main() {
	var topic string
	var broker string
	var writer bool

	flag.StringVar(&topic, "topic", "default-topic", "the topic to write to/read from")

	flag.StringVar(&broker, "broker", "default-broker", "the default broker")

	flag.BoolVar(&writer, "producer", true, "if true, produce. if false, consume")

	flag.Parse()

	fmt.Printf("The topic is: '%s'\n", topic)
	fmt.Printf("The broker is: '%s'\n", broker)
	fmt.Printf("Writer: %v\n", writer)

	if writer {

		doProducer(topic, broker)

	} else {

		doConsumer(topic, broker)

	}

}
