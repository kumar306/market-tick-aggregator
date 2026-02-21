package main

import (
	"context"
	"market-ui-backend/controller"
	"market-ui-backend/repository"
	"market-ui-backend/service"
	"os"
	"shared/logger"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {

	ctx := context.Background()
	if err := godotenv.Load(); err != nil {
		logger.Log.Error("Error in loading env. Exitting", "error", err)
		return
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

	r := gin.Default()

	api := r.Group("/api")
	{
		api.GET("/candles", ctrl.GetCandles)
	}

	r.GET("/ws", controller.HandleWebSocket)

	logger.Log.Info("UI backend running on :8080")
	r.Run(":8080")
}
