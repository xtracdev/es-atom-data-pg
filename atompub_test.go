package ecsatomdata

import (
	log "github.com/Sirupsen/logrus"
	"github.com/xtracdev/goes"
	"github.com/stretchr/testify/assert"
	"testing"
	"os"
	"errors"
	"net"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"github.com/xtracdev/pgpublish"
)


func TestSetThresholdFromEnv(t *testing.T) {
	assert.Equal(t, defaultFeedThreshold, FeedThreshold)
	os.Setenv("FEED_THRESHOLD", "2")
	ReadFeedThresholdFromEnv()
	assert.Equal(t, 2, FeedThreshold)
}

func TestSetThresholdToDefaultOnBadEnvSpec(t *testing.T) {
	os.Setenv("FEED_THRESHOLD", "two")
	ReadFeedThresholdFromEnv()
	assert.Equal(t, defaultFeedThreshold, FeedThreshold)
	os.Setenv("FEED_THRESHOLD", "2")
}

func TestReadPreviousFeedIdScanError(t *testing.T) {
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

	mock.ExpectBegin()
	mock.ExpectQuery(`select feedid from t_aefd_feed where id = \(select max\(id\) from t_aefd_feed\)`).WillReturnRows(rows)

	tx, _ := db.Begin()
	_, err = selectLatestFeed(tx)
	if assert.NotNil(t, err) {
		err = mock.ExpectationsWereMet()
		assert.Nil(t, err)
	}
}

var trueVal = true
var falseVal = false

const errorExpected = true
const noErrorExpected = false

var processTests = []struct {
	beginOk           *bool
	feedIdSelectOk    *bool
	eventInsertOk     *bool
	thesholdCountOk   *bool
	atomEventUpdateOk *bool
	feedInsertOk      *bool
	expectCommit      *bool
	expectError       bool
}{
	{&trueVal,  &trueVal, &trueVal, &trueVal, &trueVal, &trueVal, &trueVal, noErrorExpected},
	{&falseVal,  nil, nil, nil, nil, nil, nil, errorExpected},
	{&trueVal, nil, nil, nil, nil, nil, &falseVal, errorExpected},
	{&trueVal,  &falseVal, nil, nil, nil, nil, &falseVal, errorExpected},
	{&trueVal,  &trueVal, &falseVal, nil, nil, nil, &falseVal, errorExpected},
	{&trueVal, &trueVal, &trueVal, &falseVal, nil, nil, &falseVal, errorExpected},
	{&trueVal, &trueVal, &trueVal, &trueVal, &falseVal, nil, &falseVal, errorExpected},
}

func testBeginSetup(mock sqlmock.Sqlmock, ok *bool) {
	if *ok {
		mock.ExpectBegin()
	} else {
		mock.ExpectBegin().WillReturnError(errors.New("sorry mate no txn for you"))
	}
}



func testFeedIdSelectSetup(mock sqlmock.Sqlmock, ok *bool) {
	if ok == nil {
		return
	}

	if *ok == true {
		rows := sqlmock.NewRows([]string{"feedid"}).AddRow("XXX")
		mock.ExpectQuery("select feedid from t_aefd_feed").WillReturnRows(rows)
	} else {
		mock.ExpectQuery("select feedid from t_aefd_feed").WillReturnError(errors.New("BAM!"))
	}
}

func testEventInsertSetup(mock sqlmock.Sqlmock, ok *bool, eventPtr *goes.Event) {
	if ok == nil {
		return
	}

	if *ok == true {
		execOkResult := sqlmock.NewResult(1, 1)
		mock.ExpectExec("insert into t_aeae_atom_event").WithArgs(
			eventPtr.Source, eventPtr.Version, eventPtr.TypeCode, eventPtr.Payload,
		).WillReturnResult(execOkResult)
	} else {
		mock.ExpectExec("insert into t_aeae_atom_event").WillReturnError(errors.New("BAM!"))
	}
}

func testThresholdCountSetup(mock sqlmock.Sqlmock, ok *bool) {
	if ok == nil {
		return
	}
	if *ok == true {
		rows := sqlmock.NewRows([]string{"feedid"}).AddRow("XXX")
		rows = sqlmock.NewRows([]string{"count(*)"}).
			AddRow(FeedThreshold)
		mock.ExpectQuery(`select count`).WillReturnRows(rows)
	} else {
		mock.ExpectQuery(`select count`).WillReturnError(errors.New("BAM!"))
	}
}

func testThresholdAtomEventUpdateSetup(mock sqlmock.Sqlmock, ok *bool) {
	if ok == nil {
		return
	}
	if *ok == true {
		execOkResult := sqlmock.NewResult(1, 1)
		mock.ExpectExec("update t_aeae_atom_event set feedid").WillReturnResult(execOkResult)
	} else {
		mock.ExpectExec("update t_aeae_atom_event set feedid").WillReturnError(errors.New("BAM!"))
	}
}

func testExpectCommitSetup(mock sqlmock.Sqlmock, ok *bool) {
	if ok == nil {
		return
	}

	if *ok == false {
		mock.ExpectRollback()
	} else {
		mock.ExpectCommit()
	}

}

func testFeedInsertOk(mock sqlmock.Sqlmock, ok *bool) {
	if ok != nil {
		execOkResult := sqlmock.NewResult(1, 1)
		mock.ExpectExec("insert into t_aefd_feed").WillReturnResult(execOkResult).WithArgs(sqlmock.AnyArg(), "XXX")
	}
}

func TestProcessEvents(t *testing.T) {

	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	log.Infof("addr: %v", ln.LocalAddr())

	os.Setenv("STATSD_ENDPOINT", ln.LocalAddr().String())

	//Run this in the background - end of test will kill it. This is to let us have something to
	//catch any writes of statsd data. Since the data is written using udp we might not see anything
	//show up which is cool - we want to cover the instantiation
	go func() {
		buf := make([]byte, 1024)

		for {
			n, addr, err := ln.ReadFromUDP(buf)
			log.Info("Received ", string(buf[0:n]), " from ", addr)

			if err != nil {
				log.Info("Error: ", err)
			}
		}
	}()

	for _, tt := range processTests {

		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		eventPtr := &goes.Event{
			Source:   "agg1",
			Version:  1,
			TypeCode: "foo",
			Payload:  []byte("ok"),
		}

		testBeginSetup(mock, tt.beginOk)
		testFeedIdSelectSetup(mock, tt.feedIdSelectOk)
		testEventInsertSetup(mock, tt.eventInsertOk, eventPtr)
		testThresholdCountSetup(mock, tt.thesholdCountOk)
		testThresholdAtomEventUpdateSetup(mock, tt.atomEventUpdateOk)
		testFeedInsertOk(mock, tt.feedInsertOk)
		testExpectCommitSetup(mock, tt.expectCommit)

		processor := NewAtomDataProcessor(db)

		assert.Nil(t, err)

		eventMessage := pgpublish.EncodePGEvent(eventPtr.Source,eventPtr.Version,(eventPtr.Payload).([]byte),eventPtr.TypeCode)

		err = processor.ProcessMessage(eventMessage)
		if tt.expectError {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}

		err = mock.ExpectationsWereMet()
		assert.Nil(t, err)
	}
}
