package main

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/armon/go-metrics"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/xtracdev/es-atom-data-pg"
	"github.com/xtracdev/pgconn"
	"github.com/xtracdev/pgpublish"
	"os"
	"strings"
	"syscall"
	"time"
)

const (
	QueueUrlEnv         = "EVENT_QUEUE_URL"
	LogLevel            = "PG_ATOMDATA_LOG_LEVEL"
	MetricsDumpInterval = 1 * time.Minute
)

var (
	queueURL          string
	atomDataProcessor *esatomdatapg.AtomDataProcessor
	metricsSink       = metrics.NewInmemSink(MetricsDumpInterval, 2*MetricsDumpInterval)
	signal            = metrics.DefaultInmemSignal(metricsSink)
	errorCounter      = []string{"errors"}
	messagesReceived  = []string{"messages_received"}
	messagesProcessed = []string{"messages_processed"}
	messagesDeleted   = []string{"messages_deleted"}
	processingTime    = []string{"processing_time"}
)

func init() {
	//Grab queue url
	queueURL = os.Getenv(QueueUrlEnv)

	//Initialize metrics library, kick of metrics dump go routine.
	metrics.NewGlobal(metrics.DefaultConfig("atomdata"), metricsSink)
	pid := syscall.Getpid()
	log.Infof("Using %d for signal pid", pid)
	go func() {
		c := time.Tick(MetricsDumpInterval)
		for range c {
			//Signal self to dump metrics to stdout
			syscall.Kill(pid, metrics.DefaultSignal)
		}
	}()
}

func warnErrorf(format string, args ...interface{}) {
	metricsSink.IncrCounter(errorCounter, 1)
	log.Warnf(format, args...)
}

func warnErrorfWithFields(fields log.Fields, format string, args ...interface{}) {
	metricsSink.IncrCounter(errorCounter, 1)
	log.WithFields(fields).Warnf(format, args...)
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

func retryMessage(err error) bool {

	errorMsg := err.Error()
	if strings.Contains(errorMsg, "duplicate key value violates unique constraint") {
		return false
	}

	return true
}

func main() {

	log.SetFormatter(&log.JSONFormatter{})

	pgpublish.SetLogLevel(LogLevel)
	esatomdatapg.ReadFeedThresholdFromEnv()

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
		WaitTimeSeconds:     aws.Int64(10),
	}

	atomDataProcessor = esatomdatapg.NewAtomDataProcessor(postgressConnection.DB)

	log.Info("Process messages")
	for {
		log.Debug("Receieve message")
		resp, err := svc.ReceiveMessage(params)
		if err != nil {
			warnErrorf("Error receieving message: %s", err.Error())
			errorDelay()
			continue
		}

		messages := resp.Messages
		if len(messages) == 0 {
			continue
		}

		metricsSink.IncrCounter(messagesReceived, 1)

		message := *messages[0]

		loggingFields := log.Fields{"MsgId": *message.MessageId}

		log.WithFields(loggingFields).Infof("Extracting SNS message from %s", *message.MessageId)

		snsMessageOK := true
		sns, err := SNSMessageFromRawMessage(*message.Body)
		if err != nil {
			snsMessageOK = false
			warnErrorfWithFields(log.Fields{"msg id": *message.MessageId}, err.Error())
		}

		if snsMessageOK {
			log.WithFields(loggingFields).Infof("Processing message from %s", *message.MessageId)

			start := time.Now()
			err = atomDataProcessor.ProcessMessage(sns.Message)
			if err != nil {
				warnErrorfWithFields(
					loggingFields,
					"Error processing message: %s",
					err.Error(),
				)

				if retryMessage(err) == true {
					continue
				}
			} else {

				log.WithFields(loggingFields).Info("Sucessfully processed message ")

			}
			stop := time.Now()

			metricsSink.IncrCounter(messagesProcessed, 1)
			metrics.AddSample(processingTime, float32(stop.Sub(start).Nanoseconds()/1000000.0))
		}

		log.WithFields(loggingFields).Infof("Delete message %s", *message.MessageId)

		params := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: message.ReceiptHandle,
		}
		_, err = svc.DeleteMessage(params)
		if err != nil {
			warnErrorf("Error deleting message: %s", err.Error())
		} else {
			metricsSink.IncrCounter(messagesDeleted, 1)
		}
	}
}
