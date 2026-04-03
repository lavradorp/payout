package main

import (
    "payout/internal/data"
	"payout/internal/handler"

	"github.com/gin-gonic/gin"
)


func main() {
    rdb := data.InitRedisClient()
    router := gin.Default()

    router.GET("/net_yield/all", handler.GetAllNetYields(rdb))
    router.GET("/net_yield/:account_id", handler.GetNetYieldByAccountID(rdb))
    router.Run()
}