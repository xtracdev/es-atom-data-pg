package main

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/xtracdev/es-atom-data-pg"
	"github.com/xtracdev/pgconn"
	"github.com/xtracdev/pgpublish"
	"os"
	"time"
)

const (
	QueueUrlEnv = "EVENT_QUEUE_URL"
	LogLevel    = "PG_ATOMDATA_LOG_LEVEL"
)

var (
	queueURL          string
	atomDataProcessor *esatomdatapg.AtomDataProcessor
)

func init() {
	queueURL = os.Getenv(QueueUrlEnv)
}

func errorDelay() {
	time.Sleep(5 * time.Second)
}

type SNSMessage struct {
	Message string
}

func SNSMessageFromRawMessage(raw string) (*SNSMessage, error) {
	var snsMessage SNSMessage
	err := json.Unmarshal([]byte(raw), &snsMessage)
	return &snsMessage, err
}

func main() {
	pgpublish.SetLogLevel(LogLevel)

	log.Infof("Queue url: %s", queueURL)
	if queueURL == "" {
		log.Fatalf("%s must be specified in the environment", QueueUrlEnv)
	}

	log.Info("Connect to DB")
	config, err := pgconn.NewEnvConfig()
	if err != nil {
		log.Fatalf("Failed environment init: %s", err.Error())
	}

	postgressConnection, err := pgconn.OpenAndConnect(config.ConnectString(), 100)
	if err != nil {
		log.Fatalf("Failed environment init: %s", err.Error())
	}

	log.Info("Create session")
	session, err := session.NewSession()
	if err != nil {
		log.Fatal(err.Error())
	}

	svc := sqs.New(session)

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL), // Required
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(10),
	}

	atomDataProcessor = esatomdatapg.NewAtomDataProcessor(postgressConnection.DB)

	log.Info("Process messages")
	for {
		log.Debug("Receieve message")
		resp, err := svc.ReceiveMessage(params)
		if err != nil {
			log.Warnf("Error receieving message: %s", err.Error())
			errorDelay()
			continue
		}

		messages := resp.Messages
		if len(messages) == 0 {
			continue
		}

		message := *messages[0]
		log.Debugf("Message: %v", message)

		sns, err := SNSMessageFromRawMessage(*message.Body)
		if err != nil {
			log.Warn(err.Error())
			errorDelay()
			continue
		}

		err = atomDataProcessor.ProcessMessage(sns.Message)
		if err != nil {
			log.Warnf("Error processing message: %s", err.Error())
			continue
		}

		log.Debug("Delete message")

		params := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: message.ReceiptHandle,
		}
		_, err = svc.DeleteMessage(params)
		if err != nil {
			log.Warnf("Error deleting message: %s", err.Error())
		}
	}
}
