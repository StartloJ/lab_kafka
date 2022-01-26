package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

func producerBrustHandler(kw *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		log.Println("start brust publish ... !!")
		for i := 0; i <= 1000; i++ {
			key := fmt.Sprintf("Key-%d", i)
			msg := kafka.Message{
				Key:   []byte(key),
				Value: []byte(fmt.Sprint(uuid.New())),
			}
			err := kw.WriteMessages(context.Background(), msg)
			if err != nil {
				log.Println(err)
			} else {
				log.Println(i, " produced --> ", key)
			}
			time.Sleep(1 * time.Second)
		}
	})
}

func producerHandler(kw *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatalln(err)
		}
		msg := kafka.Message{
			Partition: 1,
			Key:       []byte(fmt.Sprintf("address-%s", r.RemoteAddr)),
			Value:     body,
		}
		log.Printf("You push msg like {\"address-%s\": %s}", r.RemoteAddr, body)
		err = kw.WriteMessages(r.Context(), msg)
		if err != nil {
			rw.Write([]byte(err.Error()))
			log.Fatalln(err)
		}
	})
}

func getKafkaWriter(url, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(url),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireNone,
		Async:        true,
	}
}

func main() {

	// get kafka writer using environment variables.
	kafkaUrl := os.Getenv("KAFKA_URL")
	topic := os.Getenv("KAFKA_TOPIC")

	kafkaWriter := getKafkaWriter(kafkaUrl, topic)

	defer kafkaWriter.Close()

	http.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		log.Println("Somebody call root path.")
		rw.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/pub", producerHandler(kafkaWriter))
	http.HandleFunc("/bulkpub", producerBrustHandler(kafkaWriter))

	//Run the web server.
	log.Println("Start Producer API...!")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
