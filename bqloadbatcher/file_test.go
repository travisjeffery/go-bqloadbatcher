package bqloadbatcher

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestNewFile(t *testing.T) {
	f, err := newFile("./dir-with-dashes/20160101-datasetid-tableid")

	assert.Equal(t, nil, err)
	assert.Equal(t, "datasetid", f.datasetID)
	assert.Equal(t, "tableid", f.tableID)

	f.Remove()
	f.Close()
}
