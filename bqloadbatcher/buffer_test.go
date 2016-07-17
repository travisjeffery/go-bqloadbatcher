package bqloadbatcher

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var (
	datasetID = os.Getenv("DATASET_ID")
	tableID   = os.Getenv("TABLE_ID")
)

type BufferTestSuite struct {
	suite.Suite
	buffer *buffer
}

func (suite *BufferTestSuite) SetupTest() {
	suite.buffer = newBuffer()
}

func (suite *BufferTestSuite) TearDownTest() {
	suite.buffer.clean()
}

type row struct {
	datasetID string
	tableID   string
	data      *json.RawMessage
}

func (r row) DatasetID() string {
	return r.datasetID
}

func (r row) TableID() string {
	return r.tableID
}

func (r row) Data() *json.RawMessage {
	return r.data
}

func (suite *BufferTestSuite) TestWriter() {
	f, err := suite.buffer.writer("time-ds-writer")
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), f)
}

func (suite *BufferTestSuite) TestShift() {
	_, err := suite.buffer.writer("time-ds-shift")
	assert.NoError(suite.T(), err)
	suite.buffer.shift()
	assert.Equal(suite.T(), 1, suite.buffer.iter())
	assert.Equal(suite.T(), 1, len(suite.buffer.readers(0)))
}

func (suite *BufferTestSuite) TestRemove() {
	_, err := suite.buffer.writer("time-ds-remove")
	assert.NoError(suite.T(), err)
	suite.buffer.shift()
	assert.Equal(suite.T(), 1, len(suite.buffer.readers(0)))
	suite.buffer.remove(0, "remove")
}

func TestBufferTestSuite(t *testing.T) {
	suite.Run(t, new(BufferTestSuite))
}
