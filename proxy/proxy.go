package proxy

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"git.apache.org/thrift.git/lib/go/thrift"
	log "github.com/ngaut/logging"
	"github.com/serialx/hashring"
	"gopkg.in/fatih/pool.v2"
)

var (
	errNoEmptyBackendsList = errors.New("No backend server")
)

type Proxy struct {
	conf *Config
}

func NewProxy(conf *Config) *Proxy {
	return &Proxy{
		conf: conf,
	}
}

func (proxy *Proxy) Serve() {
	pools, err := createBackendConnectionPools(proxy.conf.Backends, proxy.conf.ServerConnection)
	if err != nil {
		log.Fatal(err)
	}

	ring := hashring.New(proxy.conf.Backends)

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", proxy.conf.ServicePort))
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	log.Info("thrift proxy listen on", proxy.conf.ServicePort)
	for i := 0; ; i++ {
		conn, err := l.Accept()
		if err != nil {
			log.Error("Accept failed", err)
			break
		}
		if proxy.conf.LogAccessEvery == 0 || i%proxy.conf.LogAccessEvery == 0 {
			log.Infof("Accept connection %s -> %s", conn.RemoteAddr(), conn.LocalAddr())

		}
		go handleConnection(conn, pools, ring)
	}
}

func handleConnection(conn net.Conn, pools map[string]pool.Pool, ring *hashring.HashRing) {
	defer conn.Close()
	clientReader := bufio.NewReader(conn)

	for {
		hashKey, size, buf, err := readFromClient(clientReader)
		if err != nil {
			log.Error("Failed to read from client", err)
			return
		}
		size, buf, err = requestBackend(hashKey, size, buf, pools, ring)
		if err != nil {
			log.Error("Failed to request backend", err)
			return
		}
		// send response back to client
		sizeBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBuf, size)
		if _, err = conn.Write(buf); err != nil {
			log.Error("Failed to write frame size to client", err)
			return
		}
		if _, err := conn.Write(buf); err != nil {
			log.Error("Failed to write frame payload to client", err)
			return
		}
	}
}

func readFromClient(reader io.Reader) (hashKey, size uint32, buf []byte, err error) {
	buf = make([]byte, 4)
	if _, err = io.ReadFull(reader, buf); err != nil {
		return
	}
	size = binary.BigEndian.Uint32(buf)
	if size < 4 {
		err = thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, fmt.Sprintf("Incorrect frame size (%d)", size))
		return
	}
	buf = make([]byte, size)
	if _, err = io.ReadFull(reader, buf); err != nil {
		return
	}
	size -= 4
	hashKey = binary.BigEndian.Uint32(buf[size:])
	return hashKey, size, buf[:size], nil
}

func requestBackend(hashKey, size uint32, buf []byte, pools map[string]pool.Pool, ring *hashring.HashRing) (respSize uint32, respBuf []byte, err error) {
	// select backend
	server, ok := ring.GetNode(fmt.Sprint(hashKey))
	if !ok {
		err = fmt.Errorf("Failed to get node from hashring", hashKey)
		log.Error(err)
		return
	}
	backendConn, err := pools[server].Get()
	if err != nil {
		log.Error("Failed to get backend connection", err, server)
		return
	}
	defer backendConn.Close()
	// send to backend
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, size)
	if _, err = backendConn.Write(buf); err != nil {
		log.Error("Failed to write frame size to backend", err)
		backendConn.(*pool.PoolConn).MarkUnusable()
		return
	}
	if _, err = backendConn.Write(buf); err != nil {
		log.Error("Failed to write frame payload to backend", err)
		backendConn.(*pool.PoolConn).MarkUnusable()
		return
	}
	// read from backend
	serverReader := bufio.NewReader(backendConn)
	if _, err = io.ReadFull(serverReader, sizeBuf); err != nil {
		log.Error("Failed to read frame size", err)
		backendConn.(*pool.PoolConn).MarkUnusable()
		return
	}
	respSize = binary.BigEndian.Uint32(sizeBuf)
	if respSize < 0 {
		err = fmt.Errorf("Invalid frame size: %d", respSize)
		log.Error(err)
		backendConn.(*pool.PoolConn).MarkUnusable()
		return
	}
	respBuf = make([]byte, respSize)
	if _, err = io.ReadFull(serverReader, respBuf); err != nil {
		log.Error("Failed to read frame payload", err)
		backendConn.(*pool.PoolConn).MarkUnusable()
		return
	}
	return
}

func createBackendConnectionPools(backends []string, serverConnection int) (map[string]pool.Pool, error) {
	if len(backends) == 0 {
		log.Fatal(errNoEmptyBackendsList)
	}
	pools := make(map[string]pool.Pool)
	for _, backend := range backends {
		if p, err := pool.NewChannelPool(0, serverConnection, func() (net.Conn, error) {
			return net.Dial("tcp", backend)
		}); err != nil {
			return nil, err
		} else {
			pools[backend] = p
		}
	}
	return pools, nil
}
