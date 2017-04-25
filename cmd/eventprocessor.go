package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/service/sqs"
	"os"
	"github.com/aws/aws-sdk-go/aws"
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

func main() {
	log.Info("Create session")
	session, err := session.NewSession()
	if err != nil {
		log.Fatal(err.Error())
	}

	svc := sqs.New(session)

	params := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), // Required
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:       aws.Int64(10),
		WaitTimeSeconds:         aws.Int64(10),
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

		message := messages[0]
		log.Infof("Message: %v", message)

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
