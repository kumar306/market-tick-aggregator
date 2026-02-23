package main

import (
	"context"
	"market-ui-backend/config"
	"market-ui-backend/constants"
	"market-ui-backend/controller"
	"market-ui-backend/repository"
	"market-ui-backend/service"
	"market-ui-backend/stream"
	"os"
	"os/signal"
	"shared/logger"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {

	ctx := context.Background()
	if err := godotenv.Load(constants.EnvFile); err != nil {
		logger.Log.Error("Error in loading env. Exitting", "error", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// load the config
	cfg, err := config.GetConfig(constants.ConfigFile)
	if err != nil {
		logger.Log.Error("Failed to load ui config. Stopping main()", "err", err)
		os.Exit(1)
	}

	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		logger.Log.Error("Database url not set")
		return
	}

	pool, err := pgxpool.New(ctx, dbUrl)
	if err != nil {
		logger.Log.Error("Failed to connect to db", "error", err)
		return
	}

	repo := repository.NewMarketRepository(pool)
	svc := service.NewMarketService(repo)
	ctrl := controller.NewMarketController(svc)

	// ws endpoint reads from kafka and writes to connection
	stream.Init(ctx, cfg.KafkaConfig)
	go stream.StartConsumer(stream.Client)

	r := gin.Default()

	api := r.Group("/api")
	{
		api.GET("/candles", ctrl.GetCandles)
		api.GET("/metrics", ctrl.GetMetrics)
		api.GET("/orderbook", ctrl.GetOrderbook)
	}

	r.GET("/ws", controller.HandleWebSocket)

	logger.Log.Info("UI backend running on :8080")
	r.Run(":8080")
}
