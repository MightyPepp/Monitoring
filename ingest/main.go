package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

type Metrics struct {
	THotend  float64 `json:"t_hotend"`
	TBed     float64 `json:"t_bed"`
	Feedrate float64 `json:"feedrate"`
	FlowPct  int     `json:"flow_pct"`
	FanPct   int     `json:"fan_pct"`
	AxisX    float64 `json:"axis_x"`
	AxisY    float64 `json:"axis_y"`
	AxisZ    float64 `json:"axis_z"`
}

type TelemetryMessage struct {
	PrinterID string    `json:"printer_id"`
	JobID     string    `json:"job_id"`
	TS        time.Time `json:"ts"`
	Layer     int       `json:"layer"`
	Metrics   Metrics   `json:"metrics"`
	Event     *string   `json:"event"`
	Status    string    `json:"status"`
}

var dbConn *pgx.Conn

func mustEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("environment variable %s is not set", key)
	}
	return key
}

func buildDSN() string {
	host := mustEnv("DB_HOST")
	port := mustEnv("DB_PORT")
	user := mustEnv("DB_USER")
	password := mustEnv("DB_PASSWORD")
	dbname := mustEnv("DB_NAME")

	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname,
	)
}