package main

import (
	"bytes"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"time"
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

func main() {
	rand.Seed(time.Now().UnixNano())

	ingestURL := os.Getenv("INGEST_URL")
	if ingestURL == "" {
		ingestURL = "http://localhost:8080/api/v1/telemetry"
	}

	client := &http.Client{Timeout: 5 * time.Second}

	printerID := "PRN-01"
	jobID := "JOB-DEMO-001"

	layer := 1
	z := 0.2
	step := 0

	log.Println("starting simulator...")

	for {
		now := time.Now().UTC()

		x := 100 + 40*math.Sin(float64(step)/10.0)
		y := 100 + 30*math.Cos(float64(step)/12.0)

		msg := TelemetryMessage{
			PrinterID: printerID,
			JobID:     jobID,
			TS:        now,
			Layer:     layer,
			Status:    "printing",
			Event:     nil,
			Metrics: Metrics{
				THotend:  205 + rand.Float64()*4 - 2,
				TBed:     60 + rand.Float64()*2 - 1,
				Feedrate: 45 + rand.Float64()*10,
				FlowPct:  100,
				FanPct:   70 + rand.Intn(15),
				AxisX:    x,
				AxisY:    y,
				AxisZ:    z,
			},
		}

		body, err := json.Marshal(msg)
		if err != nil {
			log.Printf("marshal error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		resp, err := client.Post(ingestURL, "application/json", bytes.NewBuffer(body))
		if err != nil {
			log.Printf("post error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		_ = resp.Body.Close()

		log.Printf("sent telemetry: layer=%d z=%.2f http=%d", msg.Layer, msg.Metrics.AxisZ, resp.StatusCode)

		step++
		if step%8 == 0 {
			layer++
			z += 0.2
		}

		if layer > 50 {
			log.Println("simulation completed")
			break
		}

		time.Sleep(time.Second)
	}
}