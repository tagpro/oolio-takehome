# Promo Code Pre-compute Tool

This tool pre-processes coupon code files to generate a list of valid promo codes using an optimized hash partitioning algorithm.

## Valid Code Criteria

A promo code is valid if:
1. It appears in at least 2 of the 3 coupon code files
2. Its length is between 8 and 10 characters (inclusive)

## Usage

```bash
go run cmd/precompute/main.go --input coupon_codes/ --output valid_codes.txt
```

**Performance:**
- Time: ~3 minutes (for ~300M codes)
- Peak RAM: <500MB
- Uses hash partitioning for optimal speed and memory efficiency
- Output: Sorted alphabetically

## Output

Generates a single text file with one promo code per line, sorted alphabetically.

## Examples

```bash
# Using default output name
go run cmd/precompute/main.go --input coupon_codes/

# Custom output path
go run cmd/precompute/main.go --input coupon_codes/ --output results/promo_codes.txt
```

## Testing

Run unit tests:
```bash
go test ./internal/precompute -v
```

## Build

Build the executable:
```bash
go build -o precompute cmd/precompute/main.go
./precompute --input coupon_codes/ --output valid_codes
```
