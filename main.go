package main

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"os/signal"
	"time"

	events "github.com/AlchemyTelcoSolutions/events/gen/go/average_cost/v1"
	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"google.golang.org/protobuf/proto"
)

var (
	kafkaBrokers = []string{
		"b-1.kafkastaging.gtafd3.c6.kafka.eu-west-1.amazonaws.com:9098",
		"b-2.kafkastaging.gtafd3.c6.kafka.eu-west-1.amazonaws.com:9098",
	}
	KafkaTopic = "AVERAGE_COST" // change with your topic
	enqueued   int
)

type MSKAccessTokenProvider struct {
}

func (m *MSKAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthTokenFromCredentialsProvider(context.TODO(), "eu-west-1", credentials.NewStaticCredentialsProvider(
		"x", // aws access key
		"x", // aws secret key
		"x", // aws session token
	))
	return &sarama.AccessToken{Token: token}, err
}

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	producer, err := setupProducer()
	if err != nil {
		panic(err)
	} else {
		log.Println("Kafka AsyncProducer up and running!")
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	produceMessages(producer, signals)

	log.Printf("Kafka AsyncProducer finished with %d messages produced.", enqueued)

	time.Sleep(30000)
}

// setupProducer will create a AsyncProducer and returns it
func setupProducer() (sarama.SyncProducer, error) {
	// Set the SASL/OAUTHBEARER configuration
	config := sarama.NewConfig()
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	config.Net.SASL.TokenProvider = &MSKAccessTokenProvider{}
	config.Producer.Return.Successes = true

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig
	return sarama.NewSyncProducer(kafkaBrokers, config)
}

// produceMessages will send message from file publish.json to kafka topic
func produceMessages(producer sarama.SyncProducer, signals chan os.Signal) {
	data := events.AverageCost{
		NetsuiteLocationId: 43,
		NetsuiteInternalId: "36488",
		NetsuiteQty:        1,
		NetsuiteSku:        "HC-DY-389411-ITE-C",
		PricingSku:         "EP-DY-310128-ITE",
		Grade:              "CP",
		AverageCost:        221.05,
	}

	msg, err := proto.Marshal(&data)
	if err != nil {
		log.Fatalln("failed to marshal average cost event:", err)
	}

	message := &sarama.ProducerMessage{Topic: KafkaTopic, Value: sarama.ByteEncoder(msg)}
	err = producer.SendMessages([]*sarama.ProducerMessage{message})
	if err != nil {
		log.Fatalln("Error sending message:", err)
	}
}
