package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}

	address := os.Args[1]
	cmd := strings.ToLower(os.Args[2])
	base := "http://" + address
	client := &http.Client{}

	switch cmd {

	case "get":
		mustArgs(cmd, 1)
		key := os.Args[3]
		doRequest(client, http.MethodGet, base+"/api/keys/"+key, nil)

	case "set":
		mustArgs(cmd, 2)
		key := os.Args[3]
		value := os.Args[4]
		body, _ := json.Marshal(map[string]string{"value": value})
		doRequest(client, http.MethodPut, base+"/api/keys/"+key, body)

	case "delete":
		mustArgs(cmd, 1)
		key := os.Args[3]
		doRequest(client, http.MethodDelete, base+"/api/keys/"+key, nil)

	case "all":
		doRequest(client, http.MethodGet, base+"/api/keys", nil)

	case "status":
		doRequest(client, http.MethodGet, base+"/api/status", nil)

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func doRequest(client *http.Client, method, url string, body []byte) {
	var req *http.Request
	var err error

	if body != nil {
		req, err = http.NewRequest(method, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "request error:", err)
		os.Exit(1)
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection error:", err)
		fmt.Fprintln(os.Stderr, "tip: is the node running?")
		os.Exit(1)
	}
	defer resp.Body.Close()

	var result any
	json.NewDecoder(resp.Body).Decode(&result)
	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(out))
}

func mustArgs(cmd string, n int) {
	if len(os.Args) < 3+n {
		fmt.Fprintf(os.Stderr, "command %q requires %d more argument(s)\n", cmd, n)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  cli <address> status               — node state and term")
	fmt.Println("  cli <address> get <key>             — read a value")
	fmt.Println("  cli <address> set <key> <value>     — write a value")
	fmt.Println("  cli <address> delete <key>          — remove a key")
	fmt.Println("  cli <address> all                   — list all key-value pairs")
}
