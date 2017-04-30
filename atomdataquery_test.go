package esatomdatapg

import (
	"database/sql"
	"errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
	"time"
)

func TestQueryForRecent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	ts := time.Now()
	rows := sqlmock.NewRows([]string{"event_time", "aggregate_id",
		"version", "typecode", "payload"},
	).AddRow(ts, "1x2x333", 3, "foo", []byte("yeah ok"))
	mock.ExpectQuery("select").WillReturnRows(rows)

	events, err := RetrieveRecent(db)
	if assert.Nil(t, err) {
		err := mock.ExpectationsWereMet()
		assert.Nil(t, err, "mock expectations were not met")
		if assert.Equal(t, 1, len(events), "Expected an event back") {
			event := events[0]
			assert.Equal(t, event.Timestamp, ts)
			assert.Equal(t, event.Payload, []byte("yeah ok"))
			assert.Equal(t, event.TypeCode, "foo")
			assert.Equal(t, event.Source, "1x2x333")
			assert.Equal(t, event.Version, 3)
		}
	}
}

func TestQueryForRecentQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("select").WillReturnError(errors.New("boom"))

	_, err = RetrieveRecent(db)
	if assert.NotNil(t, err) {
		assert.Equal(t, "boom", err.Error())
		err = mock.ExpectationsWereMet()
		assert.Nil(t, err, "expectations not met in TestQueryForRecentQueryError")
	}
}

func TestQueryForRecentScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	foo := struct {
		foo string
		bar string
	}{
		"foo", "bar",
	}
	rows := sqlmock.NewRows([]string{"feedid"}).AddRow(foo)
	mock.ExpectQuery("select").WillReturnRows(rows)

	_, err = RetrieveRecent(db)
	if assert.NotNil(t, err) {
		err = mock.ExpectationsWereMet()
		assert.Nil(t, err, "expectations not met in TestQueryForRecentQueryError")
	}
}

func TestQueryForRecentFinalRowsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	ts := time.Now()
	rows := sqlmock.NewRows([]string{"event_time", "aggregate_id",
		"version", "typecode", "payload"},
	).AddRow(ts, "1x2x333", 3, "foo", []byte("yeah ok")).RowError(0, errors.New("dang"))
	mock.ExpectQuery("select").WillReturnRows(rows)

	_, err = RetrieveRecent(db)
	if assert.NotNil(t, err) {
		assert.Equal(t, "dang", err.Error())
		err = mock.ExpectationsWereMet()
		assert.Nil(t, err, "expectations not met in TestQueryForRecentQueryError")
	}

}

func TestQueryForLastFeed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"feedid"}).AddRow("feed-xxx")
	mock.ExpectQuery("select").WillReturnRows(rows)

	feedid, err := RetrieveLastFeed(db)
	if assert.Nil(t, err) {
		err := mock.ExpectationsWereMet()
		assert.Nil(t, err, "mock expectations were not met")
		assert.Equal(t, "feed-xxx", feedid)
	}
}

func TestQueryForLastFeedNoFeed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"feedid"})
	mock.ExpectQuery("select").WillReturnRows(rows)

	feedid, err := RetrieveLastFeed(db)
	if assert.Nil(t, err) {
		err := mock.ExpectationsWereMet()
		assert.Nil(t, err, "mock expectations were not met")
		assert.Equal(t, "", feedid)
	}
}

func TestQueryForLastFeedError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("select").WillReturnError(errors.New("dang"))

	_, err = RetrieveLastFeed(db)
	if assert.NotNil(t, err) {
		assert.Equal(t, "dang", err.Error())
		err := mock.ExpectationsWereMet()
		assert.Nil(t, err, "mock expectations were not met")

	}
}

func TestQueryForArchive(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	ts := time.Now()
	rows := sqlmock.NewRows([]string{"event_time", "aggregate_id",
		"version", "typecode", "payload"},
	).AddRow(ts, "1x2x333", 3, "foo", []byte("yeah ok"))
	mock.ExpectQuery("select").WithArgs("foo").WillReturnRows(rows)

	events, err := RetrieveArchive(db, "foo")
	if assert.Nil(t, err) {
		err := mock.ExpectationsWereMet()
		assert.Nil(t, err, "mock expectations were not met")
		if assert.Equal(t, 1, len(events), "Expected an event back") {
			event := events[0]
			assert.Equal(t, event.Timestamp, ts)
			assert.Equal(t, event.Payload, []byte("yeah ok"))
			assert.Equal(t, event.TypeCode, "foo")
			assert.Equal(t, event.Source, "1x2x333")
			assert.Equal(t, event.Version, 3)
		}
	}
}

func TestRetrievePreviousFeed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"previous"}).AddRow("baz")
	mock.ExpectQuery("select").WithArgs("bar").WillReturnRows(rows)
	previous, err := RetrievePreviousFeed(db, "bar")
	if assert.Nil(t, err) && assert.True(t, previous.Valid) {
		assert.Equal(t, "baz", previous.String)
	}
}

func TestRetrievePreviousFeedNoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"previous"})
	mock.ExpectQuery("select").WithArgs("bar").WillReturnRows(rows)
	previous, err := RetrievePreviousFeed(db, "bar")
	assert.Nil(t, err)
	assert.False(t, previous.Valid)
}

func TestRetrievePreviousFeedRowError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("select").WithArgs("bar").WillReturnError(errors.New("boom"))
	_, err = RetrievePreviousFeed(db, "bar")
	if assert.NotNil(t, err) {
		assert.Equal(t, "boom", err.Error())
	}
}

func TestRetrieveNextFeed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"feedid"}).AddRow("foo")
	mock.ExpectQuery("select").WithArgs("bar").WillReturnRows(rows)
	next, err := RetrieveNextFeed(db, "bar")
	if assert.Nil(t, err) && assert.True(t, next.Valid) {
		assert.Equal(t, "foo", next.String)
	}
}

func TestRetrieveNextFeedNoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"feedid"})
	mock.ExpectQuery("select").WithArgs("bar").WillReturnRows(rows)
	previous, err := RetrieveNextFeed(db, "bar")
	assert.Nil(t, err)
	assert.False(t, previous.Valid)
}

func TestRetrieveNextFeedRowError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("select").WithArgs("bar").WillReturnError(errors.New("boom"))
	_, err = RetrieveNextFeed(db, "bar")
	if assert.NotNil(t, err) {
		assert.Equal(t, "boom", err.Error())
	}
}

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
