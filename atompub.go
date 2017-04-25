package ecsatomdata

import (
	"database/sql"
	log "github.com/Sirupsen/logrus"
	"github.com/xtracdev/pgpublish"
	"github.com/xtracdev/goes"
)

type AtomDataProcessor struct {
	db *sql.DB
}

func NewAtomDataProcessor(db *sql.DB) *AtomDataProcessor {
	return &AtomDataProcessor{
		db:db,
	}
}

func (adp *AtomDataProcessor) ProcessMessage(msg string) error {
	log.Infof("process message %s", msg)

	var aggId, typecode string
	var version int
	var payload []byte
	var err error

	aggId, version, payload, typecode, err = pgpublish.DecodePGEvent(msg)
	if err != nil {
		return err
	}

	event := goes.Event {
		Source:aggId,
		Version:version,
		Payload: payload,
		TypeCode: typecode,
	}

	return adp.processEvent(&event)
}

func (adp *AtomDataProcessor) processEvent(event *goes.Event) error {
	log.Infof("process events: %v", event)
	return nil
}