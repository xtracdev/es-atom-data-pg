package esatomdatapg

import (
	"database/sql"
	"time"
	"github.com/xtracdev/goes"
)

const (
	sqlSelectEvent        = `select event_time, typecode, payload from t_aeae_atom_event where aggregate_id = $1 and version = $2`
)

type TimestampedEvent struct {
	goes.Event
	Timestamp time.Time
}

func RetrieveEvent(db *sql.DB, aggID string, version int) (TimestampedEvent, error) {
	var event TimestampedEvent

	var eventTime time.Time
	var typecode string
	var payload []byte

	err := db.QueryRow(sqlSelectEvent, aggID, version).Scan(&eventTime, &typecode, &payload)
	if err != nil {
		return event, err //Caller can sort out no rows vs other error
	}

	event = TimestampedEvent{
		Event: goes.Event{
			Source:   aggID,
			Version:  version,
			Payload:  payload,
			TypeCode: typecode,
		},
		Timestamp: eventTime,
	}

	return event, nil
}
