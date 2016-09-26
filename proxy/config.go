package proxy

type Config struct {
	LogAccessEvery   int
	ServicePort      int `required:"true"`
	DebugPort        int
	ServerConnection int      `default:"1000"`
	Backends         []string `required:"true"`
}
