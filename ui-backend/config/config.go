package config

import (
	"fmt"
	"market-ui-backend/constants"
	"os"
	"shared/logger"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

func GetConfig(cfgFilePath string) (*constants.Config, error) {

	loadErr := godotenv.Load(constants.EnvFile)
	if loadErr != nil {
		return nil, logger.LogAndWrap("Error loading .env file", loadErr)
	}

	var c constants.Config
	yamlFile, err := os.ReadFile(cfgFilePath)
	if err != nil {
		logger.Log.Error("Error when reading file from path",
			"configFilePath", cfgFilePath,
			"err", err)
		return nil, fmt.Errorf("error when reading ui backend config: %w", err)
	}

	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		logger.Log.Error("Error when unmarshalling YAML", "err", err)
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	// // validate correct config values
	// err = Validate(&c)
	// if err != nil {
	// 	logger.Log.Error("Validation Error", "err", err)
	// 	return nil, fmt.Errorf("validation error: %w", err)
	// }

	return &c, nil
}
