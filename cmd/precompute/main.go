package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"order-food-online/internal/precompute"
)

func main() {
	// Define command-line flags
	inputDir := flag.String("input", "", "Directory containing coupon code files (required)")
	outputFile := flag.String("output", "valid_codes.txt", "Output file path (default: valid_codes.txt)")
	flag.Parse()

	// Validate input
	if *inputDir == "" {
		fmt.Fprintf(os.Stderr, "Error: --input flag is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// Check if input directory exists
	if _, err := os.Stat(*inputDir); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: Input directory '%s' does not exist\n", *inputDir)
		os.Exit(1)
	}

	fmt.Printf("Promo Code Pre-compute Tool\n")
	fmt.Printf("============================\n\n")
	fmt.Printf("Input directory: %s\n", *inputDir)
	fmt.Printf("Output file: %s\n", *outputFile)
	fmt.Println()

	// Track start time for elapsed time reporting
	programStart := time.Now()

	// Progress callback that shows elapsed time
	progressCallback := func(msg string) {
		elapsed := time.Since(programStart)
		fmt.Printf("[%s] %s\n", formatElapsed(elapsed), msg)
	}

	// Find valid codes using hash partition
	startTime := time.Now()
	validCodes, err := precompute.FindValidCodesHashPartition(*inputDir, progressCallback)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
		os.Exit(1)
	}

	processingTime := time.Since(startTime)

	// Write output
	progressCallback("Writing output file...")

	if err := precompute.WriteTextFile(validCodes, *outputFile); err != nil {
		fmt.Fprintf(os.Stderr, "\nError writing output: %v\n", err)
		os.Exit(1)
	}

	// Summary
	fmt.Printf("\n✓ Success!\n")
	fmt.Printf("  Valid codes found: %d\n", len(validCodes))
	fmt.Printf("  Processing time: %s\n", processingTime.Round(time.Second))
	fmt.Printf("  Output file: %s\n", *outputFile)
	fmt.Println()
}

// formatElapsed formats a duration into a human-readable elapsed time string
func formatElapsed(d time.Duration) string {
	d = d.Round(time.Second)
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60

	if minutes > 0 {
		return fmt.Sprintf("%dm%02ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}
