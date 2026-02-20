package db

import (
	"context"
	"fmt"
	"market-persistence/config"
	"shared/logger"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	Pool   *pgxpool.Pool
	DbOnce sync.Once
)

func InitDB(ctx context.Context) error {
	var initErr error
	DbOnce.Do(func() {

		cfg, err := config.LoadPostgresConfig()
		if err != nil {
			initErr = logger.LogAndWrap("Error in loading postgres config", err)
			return
		}

		dsn := fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s",
			cfg.User,
			cfg.Password,
			cfg.Host,
			cfg.Port,
			cfg.Database,
		)

		poolConfig, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			initErr = logger.LogAndWrap("Error in parsing postgres dsn", err)
			return
		}

		poolConfig.MaxConns = int32(cfg.MaxConns)
		db, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			initErr = logger.LogAndWrap("Error in creating postgres pool", err)
			return
		}

		Pool = db

		deadline := time.Now().Add(10 * time.Second)
		var started bool = false
		for time.Now().Before(deadline) {
			err := db.Ping(ctx)
			if err != nil {
				logger.Log.Error("Ping to postgres failed. Sleeping for 1 second", "error", err)
				time.Sleep(1 * time.Second)
			} else {
				logger.Log.Info("Successfully pinged postgres db")
				started = true
				break
			}
		}

		if !started {
			initErr = logger.LogAndWrap("Couldn't ping postgres DB within deadline", nil)
			return
		}

		logger.Log.Info("Initialized postgres successfully", "user", cfg.User,
			"host", cfg.Host, "port", cfg.Port, "database", cfg.Database, "max_conns", cfg.MaxConns)

	})

	return initErr
}
