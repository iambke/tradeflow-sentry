package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Trade struct {
	UserID    string  `json:"user_id"`
	TradeType string  `json:"trade_type"`
	Amount    float64 `json:"amount"`
	Asset     string  `json:"asset"`
}

type Alert struct {
	AlertMsg     string  `json:"alert_msg"`
	AnomalyScore float64 `json:"anomaly_score"`
}

func hashUserID(userID string) string {
	h := sha256.Sum256([]byte(userID))
	return fmt.Sprintf("%x", h)
}

func main() {
	db, err := sql.Open("postgres", "postgres://trader:securepass@localhost:5432/trades_db?sslmode=disable")
	if err != nil {
		log.Fatal("❌ DB connection failed:", err)
	}
	defer db.Close()

	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "trades",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Accept")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	r.POST("/submitTrade", func(c *gin.Context) {
		var trade Trade
		if err := c.ShouldBindJSON(&trade); err != nil {
			log.Println("❌ Invalid trade JSON:", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		trade.UserID = hashUserID(trade.UserID)

		_, err := db.Exec(`INSERT INTO trades (user_id_hash, trade_type, amount, asset) VALUES ($1, $2, $3, $4)`,
			trade.UserID, trade.TradeType, trade.Amount, trade.Asset)
		if err != nil {
			log.Println("❌ DB insert failed:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "DB insert failed"})
			return
		}

		msg, _ := json.Marshal(trade)
		err = kafkaWriter.WriteMessages(c, kafka.Message{Value: msg})
		if err != nil {
			log.Println("❌ Kafka publish failed:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Kafka publish failed"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "trade received"})
	})

	r.GET("/getAlerts/:userID", func(c *gin.Context) {
		userID := c.Param("userID")

		rows, err := db.Query("SELECT alert_msg, anomaly_score FROM alerts WHERE user_id_hash = $1", userID)
		if err != nil {
			log.Println("❌ Alert query failed:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "DB query failed"})
			return
		}
		defer rows.Close()

		var alerts []Alert
		for rows.Next() {
			var alert Alert
			if err := rows.Scan(&alert.AlertMsg, &alert.AnomalyScore); err == nil {
				alerts = append(alerts, alert)
			}
		}

		c.JSON(http.StatusOK, alerts)
	})

	r.GET("/getTrades/:userID", func(c *gin.Context) {
		userID := c.Param("userID")

		rows, err := db.Query(`
			SELECT amount, asset, trade_type, timestamp 
			FROM trades 
			WHERE user_id_hash = $1 
			ORDER BY timestamp ASC
		`, userID)
		if err != nil {
			log.Println("❌ Trade query failed:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "DB query failed"})
			return
		}
		defer rows.Close()

		var trades []map[string]interface{}
		for rows.Next() {
			var amount float64
			var asset, tradeType string
			var timestamp string
			if err := rows.Scan(&amount, &asset, &tradeType, &timestamp); err == nil {
				trades = append(trades, gin.H{
					"amount":     amount,
					"asset":      asset,
					"trade_type": tradeType,
					"timestamp":  timestamp,
				})
			}
		}

		c.JSON(http.StatusOK, trades)
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// Graceful shutdown
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Listen error: %s\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("👋 Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
}
