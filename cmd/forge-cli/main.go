package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

var apiClient = &http.Client{Timeout: 15 * time.Second}

func main() {
	apiURL := getenv("FORGE_API_URL", "http://localhost:8080")
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "submit":
		submit(apiURL, os.Args[2:])
	case "ls":
		ls(apiURL, os.Args[2:])
	case "dlq":
		dlq(apiURL, os.Args[2:])
	case "schedules":
		schedules(apiURL, os.Args[2:])
	case "schedule":
		schedule(apiURL, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func schedules(apiURL string, args []string) {
	_ = args
	do("GET", apiURL+"/v1/schedules", nil)
}

func schedule(apiURL string, args []string) {
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("schedule create", flag.ExitOnError)
		name := fs.String("name", "", "schedule name")
		cronExpr := fs.String("cron", "", "five-field cron expression")
		queueName := fs.String("queue", "default", "queue name")
		handler := fs.String("handler", "echo", "handler name")
		payload := fs.String("payload", "{}", "JSON payload")
		_ = fs.Parse(args[1:])
		var payloadJSON json.RawMessage = []byte(*payload)
		do("POST", apiURL+"/v1/schedules", map[string]any{"name": *name, "cron_expr": *cronExpr, "queue": *queueName, "handler": *handler, "payload": payloadJSON})
	case "pause", "resume", "delete":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		method, suffix := "POST", "/"+args[0]
		if args[0] == "delete" {
			method, suffix = "DELETE", ""
		}
		do(method, apiURL+"/v1/schedules/"+args[1]+suffix, nil)
	default:
		usage()
		os.Exit(2)
	}
}

func submit(apiURL string, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	queueName := fs.String("queue", "default", "queue name")
	handler := fs.String("handler", "echo", "handler name")
	payload := fs.String("payload", "{}", "JSON payload")
	_ = fs.Parse(args)
	body := map[string]json.RawMessage{
		"payload": json.RawMessage(*payload),
	}
	queueRaw, _ := json.Marshal(*queueName)
	handlerRaw, _ := json.Marshal(*handler)
	body["queue"] = queueRaw
	body["handler"] = handlerRaw
	do("POST", apiURL+"/v1/jobs", body)
}

func ls(apiURL string, args []string) {
	_ = args
	do("GET", apiURL+"/v1/jobs?limit=50", nil)
}

func dlq(apiURL string, args []string) {
	if len(args) >= 2 && args[0] == "requeue" {
		do("POST", apiURL+"/v1/dlq/"+args[1]+"/requeue", nil)
		return
	}
	do("GET", apiURL+"/v1/dlq", nil)
}

func do(method, url string, body any) {
	var reader io.Reader
	if body != nil {
		encoded, _ := json.Marshal(body)
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		panic(err)
	}
	req.Header.Set("content-type", "application/json")
	resp, err := apiClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	fmt.Println(string(out))
	if resp.StatusCode >= 300 {
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: forge submit ... | forge ls | forge dlq [requeue <id>] | forge schedules | forge schedule create|pause|resume|delete")
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
