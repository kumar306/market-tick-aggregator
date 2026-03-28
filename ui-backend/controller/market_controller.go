package controller

import (
	"market-ui-backend/service"
	"net/http"
	"shared/logger"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type MarketController struct {
	service *service.MarketService
}

func NewMarketController(service *service.MarketService) *MarketController {
	return &MarketController{service: service}
}

func (c *MarketController) GetCandles(ctx *gin.Context) {
	exchange := ctx.Query("exchange")
	symbol := ctx.Query("symbol")
	windowID := ctx.Query("window")
	fromStr := ctx.Query("from")
	toStr := ctx.Query("to")

	from, fromErr := time.Parse(time.RFC3339, fromStr)
	if fromErr != nil {
		logger.Log.Error("Parsing error in from time", "error", fromErr)
		ctx.JSON(http.StatusBadRequest, gin.H{"error": fromErr.Error()})
		return
	}
	to, toErr := time.Parse(time.RFC3339, toStr)
	if toErr != nil {
		logger.Log.Error("Parsing error in to time", "error", toErr)
		ctx.JSON(http.StatusBadRequest, gin.H{"error": toErr.Error()})
		return
	}

	data, err := c.service.GetCandles(ctx.Request.Context(), exchange, symbol, windowID, from, to)
	if err != nil {
		logger.Log.Error("Error in controller", "error", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, data)
}

func (c *MarketController) GetMetrics(ctx *gin.Context) {
	exchange := ctx.Query("exchange")
	symbol := ctx.Query("symbol")
	fromStr := ctx.Query("from")
	toStr := ctx.Query("to")
	windows := ctx.QueryArray("windows")
	metrics := ctx.QueryArray("metrics")

	from, fromErr := time.Parse(time.RFC3339, fromStr)
	if fromErr != nil {
		logger.Log.Error("Parsing error in from time", "error", fromErr)
		ctx.JSON(http.StatusBadRequest, gin.H{"error": fromErr.Error()})
		return
	}
	to, toErr := time.Parse(time.RFC3339, toStr)
	if toErr != nil {
		logger.Log.Error("Parsing error in to time", "error", toErr)
		ctx.JSON(http.StatusBadRequest, gin.H{"error": toErr.Error()})
		return
	}

	data, err := c.service.GetMetrics(ctx.Request.Context(), exchange, symbol, windows, metrics, from, to)
	if err != nil {
		logger.Log.Error("Error in controller", "error", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, data)
}

func (c *MarketController) GetOrderbook(ctx *gin.Context) {
	exchange := ctx.Query("exchange")
	symbol := ctx.Query("symbol")
	depthStr := ctx.Query("depth")

	depth, err := strconv.Atoi(depthStr)
	if err != nil {
		logger.Log.Error("Parsing error in depth", "error", err)
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	data, err := c.service.GetOrderbook(ctx, exchange, symbol, depth)
	if err != nil {
		logger.Log.Error("Error in controller", "error", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, data)
}
