package main

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"os"
	"time"
)

const (
	QueueUrlEnv = "EVENT_QUEUE_URL"
)

var (
	queueURL string
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
		} else {
			log.Info(sns.Message)
		}

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
