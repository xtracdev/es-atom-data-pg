package esatomdatapg

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/xtracdev/envinject"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/pgpublish"
)

const (
	sqlLatestFeedId        = `select feedid from t_aefd_feed where id = (select max(id) from t_aefd_feed)`
	sqlInsertEventIntoFeed = `insert into t_aeae_atom_event (aggregate_id, version,typecode, payload, event_time) values($1,$2,$3,$4,$5)`
	sqlRecentFeedCount     = `select count(*) from t_aeae_atom_event where feedid is null`
	defaultFeedThreshold   = 100
	sqlUpdateFeedIds       = `update t_aeae_atom_event set feedid = $1 where feedid is null`
	sqlInsertFeed          = `insert into t_aefd_feed (feedid, previous) values ($1, $2)`
	EnvFeedThreshold       = "FEED_THRESHOLD"
)

type AtomDataProcessor struct {
	db            *sql.DB
	env           *envinject.InjectedEnv
	feedThreshold int
}

func NewAtomDataProcessor(db *sql.DB, env *envinject.InjectedEnv) (*AtomDataProcessor, error) {
	if env == nil {
		return nil, errors.New("Nil injected env")
	}

	threshold := readFeedThresholdFromEnv(env)

	return &AtomDataProcessor{
		db:            db,
		env:           env,
		feedThreshold: threshold,
	}, nil
}

func (adp *AtomDataProcessor) ProcessMessage(msg string) error {
	log.Infof("process message %s", msg)

	var aggId, typecode string
	var version int
	var payload []byte
	var err error
	var timestamp time.Time

	aggId, version, payload, typecode, timestamp, err = pgpublish.DecodePGEvent(msg)
	if err != nil {
		return err
	}

	event := goes.Event{
		Source:   aggId,
		Version:  version,
		Payload:  payload,
		TypeCode: typecode,
	}

	return adp.processEvent(&event, timestamp)
}

func selectLatestFeed(tx *sql.Tx) (sql.NullString, error) {
	log.Debug("Select last feed id")

	var feedid sql.NullString
	rows, err := tx.Query(sqlLatestFeedId)
	if err != nil {
		return feedid, err
	}

	defer rows.Close()
	for rows.Next() {
		//Only one row can be returned at most
		if err = rows.Scan(&feedid); err != nil {
			return feedid, err
		}
	}

	if err = rows.Err(); err != nil {
		return feedid, err
	}

	return feedid, nil
}

func doRollback(tx *sql.Tx) {
	err := tx.Rollback()
	if err != nil {
		log.Warnf("Error on transaction rollback: %s", err.Error())
	}
}

func writeEventToAtomEventTable(tx *sql.Tx, event *goes.Event, ts time.Time) error {
	log.Debug("insert event into atom_event")
	_, err := tx.Exec(sqlInsertEventIntoFeed,
		event.Source, event.Version, event.TypeCode, event.Payload, ts)
	return err
}

func getRecentFeedCount(tx *sql.Tx) (int, error) {
	log.Debug("get current count")
	var count int
	err := tx.QueryRow(sqlRecentFeedCount).Scan(&count)

	return count, err
}

func uuid() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil

}

func readFeedThresholdFromEnv(env *envinject.InjectedEnv) int {
	thresholdOverride := env.Getenv(EnvFeedThreshold)
	if thresholdOverride == "" {
		return defaultFeedThreshold
	}

	override, err := strconv.Atoi(thresholdOverride)
	if err != nil {
		log.Warnf("Attempted to override threshold with non integer: %s", thresholdOverride)
		log.Warnf("Defaulting to %d", defaultFeedThreshold)
		return defaultFeedThreshold
	}

	log.Infof("Overriding default feed threshold with %d", override)
	return override

}

func createNewFeed(tx *sql.Tx, currentFeedId sql.NullString) error {

	var prevFeedId sql.NullString
	uuidStr, err := uuid()
	if err != nil {
		return err
	}

	if currentFeedId.Valid {
		prevFeedId = currentFeedId

	}
	currentFeedId = sql.NullString{String: uuidStr, Valid: true}

	log.Info("Update feed ids")

	_, err = tx.Exec(sqlUpdateFeedIds, currentFeedId)

	if err != nil {
		return err
	}

	log.Infof("Insert into feed %v, %v", currentFeedId, prevFeedId)
	_, err = tx.Exec(sqlInsertFeed,
		currentFeedId, prevFeedId)
	return err
}

func (adp *AtomDataProcessor) processEvent(event *goes.Event, ts time.Time) error {
	log.Debugf("process event: %v", event)

	log.Debug("Processor invoked")

	//Need a transaction to group the work in this method
	log.Debug("create transaction")
	tx, err := adp.db.Begin()
	if err != nil {
		return err
	}

	//Get the current feed id
	feedid, err := selectLatestFeed(tx)
	if err != nil {
		doRollback(tx)
		return err
	}
	log.Debugf("previous feed id is %s", feedid.String)

	//Insert current row
	err = writeEventToAtomEventTable(tx, event, ts)
	if err != nil {
		doRollback(tx)
		return err
	}

	//Get current count of records in the current feed
	count, err := getRecentFeedCount(tx)
	if err != nil {
		doRollback(tx)
		return err
	}
	log.Debugf("current count is %d", count)

	//Threshold met
	if count == adp.feedThreshold {
		log.Infof("Feed threshold of %d met", adp.feedThreshold)
		err := createNewFeed(tx, feedid)
		if err != nil {
			doRollback(tx)
			return err
		}
	}

	log.Debug("commit txn")
	err = tx.Commit()
	if err != nil {
		log.Warnf("Error commiting processEvent transaction: %s", err.Error())
		return err
	}

	return nil
}
