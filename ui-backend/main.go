package main

import (
	"context"
	"errors"
	"market-ui-backend/config"
	"market-ui-backend/constants"
	"market-ui-backend/controller"
	"market-ui-backend/repository"
	"market-ui-backend/service"
	"market-ui-backend/stream"
	"net/http"
	"os"
	"os/signal"
	"shared/logger"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"strings"
)

func corsMiddleware() gin.HandlerFunc {
	allowedOrigins := map[string]struct{}{}
	origins := os.Getenv("CORS_ALLOWED_ORIGINS")
	if strings.TrimSpace(origins) == "" {
		origins = "http://localhost:3000,http://127.0.0.1:3000"
	}
	for _, origin := range strings.Split(origins, ",") {
		trimmed := strings.TrimSpace(origin)
		if trimmed == "" {
			continue
		}
		allowedOrigins[trimmed] = struct{}{}
	}

	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if _, ok := allowedOrigins[origin]; ok {
			c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
			c.Writer.Header().Set("Vary", "Origin")
			c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func main() {

	ctx := context.Background()
	if err := godotenv.Load(constants.EnvFile); err != nil {
		if os.Getenv("DATABASE_URL") == "" {
			logger.Log.Warn("Env file not loaded and DATABASE_URL not found in process environment", "error", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// load the config
	cfgPath := os.Getenv("CONFIG_FILE")
	if cfgPath == "" {
		cfgPath = constants.ConfigFile
	}
	cfg, err := config.GetConfig(cfgPath)
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
	defer pool.Close()

	repo := repository.NewMarketRepository(pool)
	svc := service.NewMarketService(repo)
	ctrl := controller.NewMarketController(svc)

	// ws endpoint reads from kafka and writes to connection
	stream.Init(ctx, cfg.KafkaConfig)
	go stream.StartConsumer(stream.Client)

	r := gin.Default()
	r.Use(corsMiddleware())

	api := r.Group("/api")
	{
		api.GET("/candles", ctrl.GetCandles)
		api.GET("/metrics", ctrl.GetMetrics)
		api.GET("/orderbook", ctrl.GetOrderbook)
	}

	r.GET("/ws", controller.HandleWebSocket)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	logger.Log.Info("UI backend running on :8080")
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Log.Error("HTTP server failed", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Log.Info("Shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Log.Error("HTTP server shutdown failed", "error", err)
		return
	}

	logger.Log.Info("UI backend stopped cleanly")
}
