package thriftext

import (
	"testing"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/collinmsn/thriftproxy/thriftproxy_test"
	"github.com/stretchr/testify/assert"
)

func TestTProxyTransport_Flush(t *testing.T) {
	assert := assert.New(t)
	req := thriftproxy_test.NewAddRequest()
	req.First, req.Second = 2, 3

	memBuffer1 := thrift.NewTMemoryBuffer()
	assert.Nil(memBuffer1.Open())
	protocol := thrift.NewTBinaryProtocol(thrift.NewTFramedTransport(memBuffer1), true, true)
	assert.Nil(req.Write(protocol))
	assert.Nil(protocol.Flush())

	memBuffer2 := thrift.NewTMemoryBuffer()
	assert.Nil(memBuffer2.Open())
	proxyTransport := NewTProxyTransport(thrift.NewTFramedTransport(memBuffer2))
	proxyTransport.SetHashKey(1234)
	protocol = thrift.NewTBinaryProtocol(proxyTransport, true, true)
	assert.Nil(req.Write(protocol))
	assert.Nil(protocol.Flush())

	assert.Equal(len(memBuffer1.Bytes()) + 4, len(memBuffer2.Bytes()))
	// frame size is different
	assert.Equal(memBuffer1.Bytes()[4:], memBuffer2.Bytes()[4:len(memBuffer1.Bytes())])
}
