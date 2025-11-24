package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dropTables = `
		DROP TABLE IF EXISTS order_items;
		DROP TABLE IF EXISTS orders;
		DROP TABLE IF EXISTS products;
	`

	createTables = `
		CREATE TABLE products (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			price REAL NOT NULL,
			category TEXT NOT NULL
		);

		CREATE TABLE orders (
			id TEXT PRIMARY KEY,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			coupon_code TEXT
		);

		CREATE TABLE order_items (
			order_id TEXT NOT NULL,
			product_id TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			FOREIGN KEY (order_id) REFERENCES orders(id),
			FOREIGN KEY (product_id) REFERENCES products(id),
			PRIMARY KEY (order_id, product_id)
		);
	`

	seedProducts = `
		-- Waffles
		INSERT INTO products (id, name, price, category) VALUES
			('1', 'Chicken Waffle', 12.99, 'Waffle'),
			('2', 'Belgian Waffle', 9.99, 'Waffle'),
			('3', 'Strawberry Waffle', 10.99, 'Waffle'),
			('4', 'Chocolate Waffle', 10.99, 'Waffle');

		-- Burgers
		INSERT INTO products (id, name, price, category) VALUES
			('5', 'Classic Burger', 8.99, 'Burger'),
			('6', 'Cheese Burger', 9.99, 'Burger'),
			('7', 'Veggie Burger', 8.49, 'Burger'),
			('8', 'Bacon Burger', 11.99, 'Burger');

		-- Drinks
		INSERT INTO products (id, name, price, category) VALUES
			('9', 'Coffee', 3.99, 'Drink'),
			('10', 'Orange Juice', 4.49, 'Drink'),
			('11', 'Soda', 2.99, 'Drink'),
			('12', 'Iced Tea', 3.49, 'Drink');

		-- Desserts
		INSERT INTO products (id, name, price, category) VALUES
			('13', 'Ice Cream', 5.99, 'Dessert'),
			('14', 'Brownie', 6.49, 'Dessert'),
			('15', 'Cheesecake', 7.99, 'Dessert'),
			('16', 'Apple Pie', 6.99, 'Dessert');
	`
)

func main() {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./food.db"
	}

	log.Printf("Setting up database at: %s\n", dbPath)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Drop existing tables
	log.Println("Dropping existing tables...")
	if _, err := db.Exec(dropTables); err != nil {
		log.Fatalf("Failed to drop tables: %v", err)
	}

	// Create tables
	log.Println("Creating tables...")
	if _, err := db.Exec(createTables); err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	// Seed products
	log.Println("Seeding products...")
	if _, err := db.Exec(seedProducts); err != nil {
		log.Fatalf("Failed to seed products: %v", err)
	}

	log.Println("Database setup completed successfully!")
	fmt.Println("\nSample products added:")
	fmt.Println("- 4 Waffles (Chicken, Belgian, Strawberry, Chocolate)")
	fmt.Println("- 4 Burgers (Classic, Cheese, Veggie, Bacon)")
	fmt.Println("- 4 Drinks (Coffee, Orange Juice, Soda, Iced Tea)")
	fmt.Println("- 4 Desserts (Ice Cream, Brownie, Cheesecake, Apple Pie)")
}
