package esatomdatapg


import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
	"time"
)



func TestRetrieveEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	ts := time.Now()
	rows := sqlmock.NewRows([]string{"event_time",
		"typecode", "payload"},
	).AddRow(ts, "foo", []byte("yeah ok"))
	mock.ExpectQuery("select").WillReturnRows(rows)

	event, err := RetrieveEvent(db, "1x2x333", 3)
	if assert.Nil(t, err) {
		err := mock.ExpectationsWereMet()
		if assert.Nil(t, err, "mock expectations were not met") {
			assert.Equal(t, event.Timestamp, ts)
			assert.Equal(t, event.Payload, []byte("yeah ok"))
			assert.Equal(t, event.TypeCode, "foo")
			assert.Equal(t, event.Source, "1x2x333")
			assert.Equal(t, event.Version, 3)
		}
	}
}

func TestRetrieveEventNoRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"event_time",
		"typecode", "payload"})
	mock.ExpectQuery("select").WillReturnRows(rows)

	_, err = RetrieveEvent(db, "1x2x333", 3)
	if assert.NotNil(t, err) {
		assert.Equal(t, sql.ErrNoRows, err)
	}
}