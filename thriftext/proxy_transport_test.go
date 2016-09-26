package thriftext

import (
	"testing"

	"fmt"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/collinmsn/thriftproxy/example"
	"github.com/collinmsn/thriftproxy/proxy"
	log "github.com/ngaut/logging"
	"github.com/stretchr/testify/assert"
)

func TestTProxyTransport_Flush(t *testing.T) {
	assert := assert.New(t)
	req := example.NewAddRequest()
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

	assert.Equal(len(memBuffer1.Bytes())+4, len(memBuffer2.Bytes()))
	// frame size is different
	assert.Equal(memBuffer1.Bytes()[4:], memBuffer2.Bytes()[4:len(memBuffer1.Bytes())])
}

type AdderHandler struct {
}

func (h *AdderHandler) Add(req *example.AddRequest) (r *example.AddResponse, err error) {
	r = example.NewAddResponse()
	r.Sum = req.First + req.Second
	return
}
func ExampleNewTProxyTransport() {
	backendServerPort := 12306
	proxyServerPort := 12307
	startBackendServer := func() {
		serverTransport, err := thrift.NewTServerSocket(fmt.Sprintf(":%d", backendServerPort))
		if err != nil {
			panic(err)
		}
		if err := serverTransport.Open(); err != nil {
			panic(err)
		}
		server := thrift.NewTSimpleServer4(example.NewAdderProcessor(&AdderHandler{}), serverTransport,
			thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()), thrift.NewTBinaryProtocolFactory(true, true))
		server.Serve()
	}
	startProxyServer := func() {
		conf := &proxy.Config{
			ServicePort:      proxyServerPort,
			ServerConnection: 100,
			Backends:         []string{fmt.Sprintf("127.0.0.1:%d", backendServerPort)},
		}
		proxy := proxy.NewProxy(conf)
		proxy.Serve()
	}
	go startBackendServer()
	go startProxyServer()
	// wait for servers ready
	time.Sleep(500 * time.Microsecond)

	sock, err := thrift.NewTSocket(fmt.Sprintf("127.0.0.1:%d", proxyServerPort))
	if err != nil {
		log.Error("Create client socket failed", err)
		return
	}
	proxyTransport := NewTProxyTransport(thrift.NewTFramedTransport(sock))
	if err := proxyTransport.Open(); err != nil {
		log.Error("Open transport to proxy server failed", err)
		return
	}
	defer proxyTransport.Close()
	proxyTransport.SetHashKey(1234)
	client := example.NewAdderClientFactory(proxyTransport, thrift.NewTBinaryProtocolFactory(true, true))

	req := &example.AddRequest{First: 2, Second: 3}
	resp, err := client.Add(req)
	if err != nil {
		log.Error("Rpc failed", err)
		return
	}

	req.Second = 4
	proxyTransport.SetHashKey(2345)
	resp2, err := client.Add(req)
	if err != nil {
		log.Error("Rpc failed", err)
		return
	}
	// Output: Sum1: 5, Sum2: 6
	fmt.Printf("Sum1: %d, Sum2: %d", resp.Sum, resp2.Sum)
}
