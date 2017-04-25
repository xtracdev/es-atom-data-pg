package main

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"os"
	"time"
	"github.com/xtracdev/ecs-atom-data"
	"github.com/xtracdev/pgconn"
)

const (
	QueueUrlEnv = "EVENT_QUEUE_URL"
)

var (
	queueURL string
	atomDataProcessor *ecsatomdata.AtomDataProcessor
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
	log.Info("Connect to DB")
	config, err := pgconn.NewEnvConfig()
	if err != nil {
		log.Warnf("Failed environment init: %s", err.Error())
	}

	postgressConnection,err := pgconn.OpenAndConnect(config.ConnectString(),1)
	if err != nil {
		log.Warnf("Failed environment init: %s", err.Error())
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

	atomDataProcessor = ecsatomdata.NewAtomDataProcessor(postgressConnection.DB)

	log.Info("Process messages")
	for {
		log.Info("Receieve message")
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
		log.Infof("Message: %v", message)

		sns, err := SNSMessageFromRawMessage(*message.Body)
		if err != nil {
			log.Warn(err.Error())
			errorDelay()
			continue
		}

		atomDataProcessor.ProcessMessage(sns.Message)

		log.Info("Delete message")

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
