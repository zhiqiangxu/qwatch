package config

// Value contains all config info
type Value struct {
	Env         string
	Servers     []string
	EnableDebug bool
}

var (
	config Value
)

const (
	// ProdEnv is the production string for prod
	ProdEnv = "prod"
	// DevEnv is for dev
	DevEnv = "dev"
)

// Load init conf for environment
func Load(env string) (*Value, error) {
	err := DecodeTOMLFile("pkg/config/"+env+".toml", &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// LoadFile loads the specified config, used in test
func LoadFile(file string) (*Value, error) {
	err := DecodeTOMLFile(file, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// Get fetches the config value
func Get() *Value {
	return &config
}
