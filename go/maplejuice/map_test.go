package maplejuice

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMapper(t *testing.T) {
	output, err := executeAndGetOutput("./wordcount_mapper.py", []string{"Hello world"})
	assert.Nil(t, err)
	log.Println(output)
	assert.Equal(t, output["Hello"], []string{"1"})
	assert.Equal(t, output["world"], []string{"1"})
}
