package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	// "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//Переписать под пул соединений с БД, пока только нужный пакет драйвера добавлен

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

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.statusCode = code
	sr.ResponseWriter.WriteHeader(code)
}

var dbPool *pgxpool.Pool

func mustEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("environment variable %s is not set", key)
	}
	return value
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

func connectDB() *pgxpool.Pool {
	dsn := buildDSN()

	var pool *pgxpool.Pool
	var err error

	for i := 1; i <= 15; i++ {
		pool, err = pgxpool.New(context.Background(), dsn)
		if err  == nil {
			err = pool.Ping(context.Background())
			if err == nil {
				log.Println("Connect to DB (pool)")
				return pool
			}
		}
		log.Printf("database is not ready yet, attemp %d/15: %v", i, err)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("failed to connect to database: %v", err)
	return nil
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Second)
	defer cancel()

	err := dbPool.Ping(ctx)
	if err != nil {
		http.Error(w, "DB not avaliable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func telemetryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg TelemetryMessage

	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if msg.PrinterID == "" || msg.JobID == "" || msg.Status == "" {
		http.Error(w, "printer_id, job_id and status are required", http.StatusBadRequest)
		return
	}

	if msg.TS.IsZero() {
		msg.TS = time.Now().UTC()
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5 * time.Second)
	defer cancel()

	_, err = dbPool.Exec(ctx, `
		INSERT INTO printer_telemetry (
			ts, printer_id, job_id, layer, 
			t_hotend, t_bed, feedrate, flow_pct, fan_pct,
			axis_x, axis_y, axiz_z,
			status, event
		) VALUES (
		 	$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			$12, $13, $14
		)
	`,
		msg.TS,
		msg.PrinterID,
		msg.JobID,
		msg.Layer,
		msg.Metrics.THotend,
		msg.Metrics.TBed,
		msg.Metrics.Feedrate,
		msg.Metrics.FlowPct,
		msg.Metrics.FanPct,
		msg.Metrics.AxisX,
		msg.Metrics.AxisY,
		msg.Metrics.AxisZ,
		msg.Status,
		msg.Event,
	)
	if err != nil {
		log.Printf("Failed to insert telemetry: %v", err)
		http.Error(w, "Failed to insert telemetry", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		recorder := &statusRecorder{
			ResponseWriter: w,
			statusCode: 	http.StatusOK,
		}

		next.ServeHTTP(recorder, r)

		log.Printf(
			"%s %s %d %s",
			r.Method,
			r.URL.Path,
			recorder.statusCode,
			time.Since(start),
		)
	})
}

func main() {
	dbPool = connectDB()
	defer dbPool.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/api/v1/telemetry", telemetryHandler)

	addr := ":8080"
	log.Printf("Starting ingest service on %s", addr)
	
	handler := loggingMiddleware(mux)

	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("Server stopped: %v", err)
	}
}