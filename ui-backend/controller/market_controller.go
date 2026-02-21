package controller

import (
	"market-ui-backend/service"
	"net/http"
	"shared/logger"
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
	fromStr := ctx.Query("from")
	toStr := ctx.Query("to")

	from, fromErr := time.Parse(time.RFC3339, fromStr)
	if fromErr != nil {
		logger.Log.Error("Parsing error in from time", "error", fromErr)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": fromErr.Error()})
		return
	}
	to, toErr := time.Parse(time.RFC3339, toStr)
	if toErr != nil {
		logger.Log.Error("Parsing error in to time", "error", toErr)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": toErr.Error()})
		return
	}

	data, err := c.service.GetCandles(ctx.Request.Context(), exchange, symbol, from, to)
	if err != nil {
		logger.Log.Error("Error in controller", "error", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, data)
}

func HandleWebSocket(ctx *gin.Context) {

}
