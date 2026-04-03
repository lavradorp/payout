package handler

import (
	"context"
	"net/http"
	"payout/internal/data"
	"payout/internal/models"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func GetAllNetYields(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		pattern := "payout:client:*:net_yield"

		data, err := data.ScanAndGetRedisData(ctx, rdb, pattern)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Erro ao consultar o cache: " + err.Error()})
			return
		}
		var results []models.ClientNetYield

		for fullKey, value := range data {
			parts := strings.Split(fullKey, ":")
			accountID := ""
			if len(parts) >= 3 {
				accountID = parts[2]
			}

			netYield := "0"
			if value != "" {
				netYield = value
			}

			results = append(results, models.ClientNetYield{
				AccountID: accountID,
				NetYield:  netYield,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"total": len(results),
			"data":  results,
		})
	}
}

func GetNetYieldByAccountID(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		key := c.Param("account_id")
		pattern := "payout:client:" + key + ":net_yield"

		net_yield, err := data.GetRedisData(ctx, rdb, pattern)

		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"erro": err.Error()})

		}

		c.JSON(http.StatusOK, gin.H{
			"account_id": key,
			"net_yield": net_yield,
		})

	}
	
}