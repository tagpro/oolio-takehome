package api

import (
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// InitDB initializes and returns a SQLite database connection
func InitDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// GetAllProducts fetches all products from the database
func GetAllProducts(db *sql.DB) ([]Product, error) {
	query := `SELECT id, name, price, category FROM products ORDER BY category, name`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query products: %w", err)
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		var id, name, category string
		var price float32

		if err := rows.Scan(&id, &name, &price, &category); err != nil {
			return nil, fmt.Errorf("failed to scan product: %w", err)
		}

		p.Id = &id
		p.Name = &name
		p.Price = &price
		p.Category = &category

		products = append(products, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating products: %w", err)
	}

	return products, nil
}

// GetProductByID fetches a single product by its ID
func GetProductByID(db *sql.DB, id string) (*Product, error) {
	query := `SELECT id, name, price, category FROM products WHERE id = ?`

	var p Product
	var productID, name, category string
	var price float32

	err := db.QueryRow(query, id).Scan(&productID, &name, &price, &category)
	if err == sql.ErrNoRows {
		return nil, nil // Product not found
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query product: %w", err)
	}

	p.Id = &productID
	p.Name = &name
	p.Price = &price
	p.Category = &category

	return &p, nil
}

// GetProductsByIDs fetches multiple products by their IDs
func GetProductsByIDs(db *sql.DB, ids []string) ([]Product, error) {
	if len(ids) == 0 {
		return []Product{}, nil
	}

	// Build query with placeholders
	query := `SELECT id, name, price, category FROM products WHERE id IN (`
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		if i > 0 {
			query += ", "
		}
		query += "?"
		args[i] = id
	}
	query += ")"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query products: %w", err)
	}
	defer rows.Close()

	products := make([]Product, 0, len(ids))
	for rows.Next() {
		var p Product
		var id, name, category string
		var price float32

		if err := rows.Scan(&id, &name, &price, &category); err != nil {
			return nil, fmt.Errorf("failed to scan product: %w", err)
		}

		p.Id = &id
		p.Name = &name
		p.Price = &price
		p.Category = &category

		products = append(products, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating products: %w", err)
	}

	return products, nil
}

// OrderItem represents an item in an order
type OrderItem struct {
	ProductID string
	Quantity  int
}

// CreateOrder creates a new order with the given items and returns the order ID
func CreateOrder(db *sql.DB, couponCode *string, items []OrderItem) (string, error) {
	// Generate UUID for the order
	orderID := uuid.New().String()

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert order
	insertOrderQuery := `INSERT INTO orders (id, coupon_code) VALUES (?, ?)`
	if _, err := tx.Exec(insertOrderQuery, orderID, couponCode); err != nil {
		return "", fmt.Errorf("failed to insert order: %w", err)
	}

	// Insert order items
	insertItemQuery := `INSERT INTO order_items (order_id, product_id, quantity) VALUES (?, ?, ?)`
	for _, item := range items {
		if _, err := tx.Exec(insertItemQuery, orderID, item.ProductID, item.Quantity); err != nil {
			return "", fmt.Errorf("failed to insert order item: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return orderID, nil
}

// ValidateProductsExist checks if all product IDs exist in the database
func ValidateProductsExist(db *sql.DB, productIDs []string) error {
	if len(productIDs) == 0 {
		return fmt.Errorf("no products specified")
	}

	// Build query with placeholders
	query := `SELECT COUNT(*) FROM products WHERE id IN (`
	args := make([]interface{}, len(productIDs))
	for i, id := range productIDs {
		if i > 0 {
			query += ", "
		}
		query += "?"
		args[i] = id
	}
	query += ")"

	var count int
	err := db.QueryRow(query, args...).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to validate products: %w", err)
	}

	if count != len(productIDs) {
		return fmt.Errorf("one or more products not found")
	}

	return nil
}
