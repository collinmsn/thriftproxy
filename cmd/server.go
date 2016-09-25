package main

import (
	"github.com/collinmsn/thriftproxy/proxy"
	"github.com/koding/multiconfig"
)

func main() {
	m := multiconfig.New()
	conf := new(proxy.Config)
	m.MustLoad(conf)
	proxy := proxy.NewProxy(conf)
	proxy.Serve()
}
