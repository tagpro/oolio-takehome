package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/http"
	"order-food-online/internal/api"
	"os"

	"github.com/go-chi/chi/v5"
)

func main() {
	promoCodesFile := flag.String("promocodes", "valid_codes.txt", "Path to the promo codes file")
	flag.Parse()

	// Load promo codes
	codes, err := loadPromoCodes(*promoCodesFile)
	if err != nil {
		log.Fatalf("Failed to load promo codes: %v", err)
	}
	log.Printf("Loaded %d promo codes", len(codes))

	// Initialize database
	dbPath := getDBPath()
	log.Printf("Connecting to database: %s", dbPath)
	db, err := api.InitDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Create server with database connection
	server := api.NewServer(codes, db)

	mux := chi.NewMux()
	h := api.HandlerFromMux(server, mux)

	s := &http.Server{
		Addr:    ":8080",
		Handler: h,
	}

	fmt.Println("Starting server on :8080")
	if err := s.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func loadPromoCodes(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open promo codes file: %w", err)
	}
	defer file.Close()

	var codes []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			codes = append(codes, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading promo codes file: %w", err)
	}

	return codes, nil
}

func getDBPath() string {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./food.db"
	}
	return dbPath
}
