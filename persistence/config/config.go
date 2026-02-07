package config

import (
	"os"
	"shared/logger"
	"strconv"
)

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	MaxConns int
}

func LoadPostgresConfig() (*PostgresConfig, error) {
	u, err := mustEnv("POSTGRES_USER")
	if err != nil {
		return nil, err
	}

	p, err := mustEnv("POSTGRES_PASSWORD")
	if err != nil {
		return nil, err
	}

	d, err := mustEnv("POSTGRES_DB")
	if err != nil {
		return nil, err
	}

	return &PostgresConfig{
		Host:     getEnv("POSTGRES_HOST", "localhost"),
		Port:     getEnvInt("POSTGRES_PORT", 5432),
		User:     u,
		Password: p,
		Database: d,
		MaxConns: getEnvInt("POSTGRES_MAX_CONNS", 10),
	}, nil
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func mustEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return v, logger.LogAndWrap("Missing env variable for key. Exiting.", nil, "key", key)
	}
	return v, nil
}
