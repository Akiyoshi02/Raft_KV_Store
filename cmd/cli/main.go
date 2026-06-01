package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}

	address := os.Args[1]
	cmd := strings.ToLower(os.Args[2])
	base := "http://" + address
	client := &http.Client{Timeout: 5 * time.Second}

	switch cmd {

	case "get":
		requireArgs(cmd, 1)
		key := url.PathEscape(os.Args[3])
		doRequest(client, http.MethodGet, base+"/api/keys/"+key, nil)

	case "set":
		mustArgs(cmd, 2)
		key := url.PathEscape(os.Args[3])
		value := strings.Join(os.Args[4:], " ")
		body, _ := json.Marshal(map[string]string{"value": value})
		doRequest(client, http.MethodPut, base+"/api/keys/"+key, body)

	case "delete":
		requireArgs(cmd, 1)
		key := url.PathEscape(os.Args[3])
		doRequest(client, http.MethodDelete, base+"/api/keys/"+key, nil)

	case "all":
		requireArgs(cmd, 0)
		doRequest(client, http.MethodGet, base+"/api/keys", nil)

	case "status":
		requireArgs(cmd, 0)
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
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "request error:", err)
		os.Exit(1)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection error:", err)
		fmt.Fprintln(os.Stderr, "tip: is the node running?")
		os.Exit(1)
	}
	defer resp.Body.Close()

	var result any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Fprintln(os.Stderr, "response error:", err)
		os.Exit(1)
	}
	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(out))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		os.Exit(1)
	}
}

func mustArgs(cmd string, n int) {
	if len(os.Args) < 3+n {
		fmt.Fprintf(os.Stderr, "command %q requires %d more argument(s)\n", cmd, n)
		os.Exit(1)
	}
}

func requireArgs(cmd string, n int) {
	mustArgs(cmd, n)
	if len(os.Args) > 3+n {
		fmt.Fprintf(os.Stderr, "command %q accepts exactly %d argument(s)\n", cmd, n)
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
