package go_callback_adapter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// Callback is the interface that Go agents must implement
type Callback interface {
	HandleRequest(message map[string]interface{}) (map[string]interface{}, error)
}

// Adapter wraps a callback and handles stdio communication
type Adapter struct {
	callback Callback
}

// Request represents a task request from the supervisor
type Request struct {
	ID      string                 `json:"id"`
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

// Response represents a response to the supervisor
type Response struct {
	ID     string                 `json:"id"`
	Status string                 `json:"status"`
	Result map[string]interface{} `json:"result,omitempty"`
	Error  string                 `json:"error,omitempty"`
}

// NewAdapter creates a new adapter with the given callback
func NewAdapter(callback Callback) *Adapter {
	return &Adapter{callback: callback}
}

// Run starts the adapter, reading from stdin and writing to stdout
func (a *Adapter) Run() error {
	// Set up logging to stderr
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Read from stdin line by line
	scanner := bufio.NewScanner(os.Stdin)
	
	log.Println("Go callback adapter started, waiting for requests...")

	for {
		// Check for shutdown signal
		select {
		case <-sigChan:
			log.Println("Received shutdown signal")
			return nil
		default:
		}

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				if err == io.EOF {
					log.Println("EOF reached, shutting down")
					return nil
				}
				return fmt.Errorf("error reading stdin: %w", err)
			}
			break
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Parse request
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			log.Printf("ERROR: Failed to parse request: %v", err)
			// Send error response if we can't parse the request
			errorResp := Response{
				ID:     "unknown",
				Status: "error",
				Error:  fmt.Sprintf("Failed to parse request: %v", err),
			}
			if err := a.sendResponse(errorResp); err != nil {
				log.Printf("ERROR: Failed to send error response: %v", err)
			}
			continue
		}

		// Handle the request
		go a.handleRequest(req)
	}

	return nil
}

// handleRequest processes a single request
func (a *Adapter) handleRequest(req Request) {
	log.Printf("Processing request ID: %s", req.ID)

	// Call the callback
	result, err := a.callback.HandleRequest(req.Payload)
	if err != nil {
		log.Printf("ERROR: Callback returned error: %v", err)
		errorResp := Response{
			ID:     req.ID,
			Status: "error",
			Error:  err.Error(),
		}
		if err := a.sendResponse(errorResp); err != nil {
			log.Printf("ERROR: Failed to send error response: %v", err)
		}
		return
	}

	// Send success response
	successResp := Response{
		ID:     req.ID,
		Status: "ok",
		Result: result,
	}
	if err := a.sendResponse(successResp); err != nil {
		log.Printf("ERROR: Failed to send success response: %v", err)
	}
}

// sendResponse writes a JSON response to stdout
func (a *Adapter) sendResponse(resp Response) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	// Write to stdout with newline
	fmt.Println(string(data))
	return nil
}

