package client

// RegCmd is struct for register
type RegCmd struct {
	Service string
	Addrs   []string
}

// LWCmd is struct for list and watch
type LWCmd struct {
	Services []string
}
