package proxy

type Config struct {
	LogAccessEvery   int
	ServicePort      int
	DebugPort        int
	ServerConnection int
	Backends         []string
}
