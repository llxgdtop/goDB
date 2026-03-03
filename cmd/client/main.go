package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strings"

	"github.com/chzyer/readline"
)

const defaultHost = "localhost"
const defaultPort = 8080
const responseEnd = "!!!end!!!"

var txnVersionRegex = regexp.MustCompile(`TRANSACTION (\d+) (BEGIN|COMMIT|ROLLBACK)`)

func main() {
	// Connect to server
	addr := fmt.Sprintf("%s:%d", defaultHost, defaultPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to %s\n", addr)
	fmt.Println("Type SQL statements or 'quit' to exit.")

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	var txnVersion uint64

	// Create readline instance
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "godb> ",
		HistoryFile:     "/tmp/godb_history.tmp",
		AutoComplete:    nil,
		InterruptPrompt: "^C",
		EOFPrompt:       "quit",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize readline: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	for {
		// Update prompt based on transaction state
		if txnVersion > 0 {
			rl.SetPrompt(fmt.Sprintf("godb#%d> ", txnVersion))
		} else {
			rl.SetPrompt("godb> ")
		}

		line, err := rl.Readline()
		if err != nil {
			// Handle interrupt or EOF
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Handle quit command
		if strings.ToUpper(line) == "QUIT" || strings.ToUpper(line) == "EXIT" {
			// Send quit to server
			writer.WriteString("QUIT\n")
			writer.Flush()
			break
		}

		// Send to server
		writer.WriteString(line + "\n")
		if err := writer.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "Write error: %v\n", err)
			break
		}

		// Read response lines until end marker
		for {
			response, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Fprintf(os.Stderr, "Read error: %v\n", err)
				}
				break
			}

			// Check for end marker
			cleanResponse := strings.TrimSuffix(response, "\n")
			if cleanResponse == responseEnd {
				break
			}

			fmt.Println(cleanResponse)

			// Check for transaction state changes
			matches := txnVersionRegex.FindStringSubmatch(cleanResponse)
			if len(matches) == 3 {
				version := parseUint64(matches[1])
				action := matches[2]
				switch action {
				case "BEGIN":
					txnVersion = version
				case "COMMIT", "ROLLBACK":
					txnVersion = 0
				}
			}
		}
	}

	fmt.Println("Disconnected.")
}

func parseUint64(s string) uint64 {
	var n uint64
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + uint64(c-'0')
		}
	}
	return n
}
