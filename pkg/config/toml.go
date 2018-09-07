package config

import (
	"github.com/BurntSushi/toml"
)

// DecodeTOMLFile decodes TOML config
func DecodeTOMLFile(path string, result interface{}) error {
	if _, err := toml.DecodeFile(path, result); err != nil {
		return err
	}

	return nil
}

// DecodeTOML decodes TOML config
func DecodeTOML(data string, result interface{}) error {
	if _, err := toml.Decode(data, result); err != nil {
		return err
	}

	return nil
}
