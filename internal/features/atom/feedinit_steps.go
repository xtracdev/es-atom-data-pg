package atom

import (
	"database/sql"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	ad "github.com/xtracdev/es-atom-data-pg"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/pgconn"
	"github.com/xtracdev/pgpublish"
)

func init() {

	var atomProcessor *ad.AtomDataProcessor

	var initFailed bool
	log.Info("Init test envionment")
	config, err := pgconn.NewEnvConfig()
	if err != nil {
		log.Warnf("Failed environment init: %s", err.Error())
		initFailed = true
	}

	db, err := pgconn.OpenAndConnect(config.ConnectString(), 1)
	if err != nil {
		log.Warnf("Failed environment init: %s", err.Error())
		initFailed = true
	}

	Given(`^a new feed environment$`, func() {

		if initFailed {
			T.Errorf("Failed init")
			return
		}

		if assert.Nil(T, err) {
			_, err = db.Exec("delete from t_aeae_atom_event")
			assert.Nil(T, err)
			_, err = db.Exec("delete from t_aefd_feed")
			assert.Nil(T, err)
		}
	})

	When(`^we start up the feed processor$`, func() {
		atomProcessor = ad.NewAtomDataProcessor(db.DB)
	})

	And(`^some events are published$`, func() {
		eventPtr := &goes.Event{
			Source:   "agg1",
			Version:  1,
			TypeCode: "foo",
			Payload:  []byte("ok"),
		}
		encodedEvent := pgpublish.EncodePGEvent(eventPtr.Source, eventPtr.Version, (eventPtr.Payload).([]byte), eventPtr.TypeCode)
		err = atomProcessor.ProcessMessage(encodedEvent)
		assert.Nil(T, err)
	})

	And(`^the number of events is lower than the feed threshold$`, func() {
		//Here we use the known starting state with the assumption our feed threshold is > 1
	})

	Then(`^the events are stored in the atom_event table with a null feed id$`, func() {
		var feedid sql.NullString
		err := db.QueryRow("select feedid from t_aeae_atom_event where aggregate_id = 'agg1'").Scan(&feedid)
		assert.Nil(T, err)
		assert.False(T, feedid.Valid)
	})

	And(`^there are no records in the feed table$`, func() {
		var count = -1
		err := db.QueryRow("select count(*) from t_aefd_feed").Scan(&count)
		if assert.Nil(T, err) {
			assert.Equal(T, count, 0)
		}
	})
}
