package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := os.Getenv("AQE_DB_PATH")
	if dbPath == "" {
		dbPath = "./data/aqe.sqlite" // Updated to match Docker volume mount path
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`DROP TABLE IF EXISTS purchases`); err != nil {
		log.Fatalf("drop: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE purchases (
        id INTEGER PRIMARY KEY,
        dt TEXT,
        country TEXT,
        amount REAL
    )`); err != nil {
		log.Fatalf("create: %v", err)
	}

	rand.Seed(42)
	countries := []string{"US", "IN", "DE", "FR", "GB", "BR", "CA", "AU", "JP", "MX"}
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO purchases(dt,country,amount) VALUES (?,?,?)")
	defer stmt.Close()

	n := 200000 // 200k rows for quick demo
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		d := start.Add(time.Duration(rand.Intn(365*24)) * time.Hour)
		c := countries[rand.Intn(len(countries))]
		// amount heavy-tail
		amt := 10 + rand.ExpFloat64()*50
		if _, err := stmt.Exec(d.Format(time.RFC3339), c, amt); err != nil {
			log.Fatalf("insert: %v", err)
		}
		if i%10000 == 0 {
			fmt.Printf("inserted %d\n", i)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatalf("commit: %v", err)
	}
	fmt.Println("Seed done.")

	// Create demo tables for strategy selection demos
	if err := createDemoTables(db); err != nil {
		log.Fatalf("Failed to create demo tables: %v", err)
	}
	fmt.Println("Demo tables created successfully!")
}

// createDemoTables creates additional tables for demo scripts
func createDemoTables(db *sql.DB) error {
	log.Println("Creating demo tables for strategy selection...")

	// Create large_sales table
	if _, err := db.Exec(`DROP TABLE IF EXISTS large_sales`); err != nil {
		return fmt.Errorf("drop large_sales: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE large_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL,
        order_date DATE NOT NULL,
        amount REAL NOT NULL,
        region TEXT NOT NULL,
        product_category TEXT NOT NULL,
        sales_rep_id INTEGER,
        payment_method TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`); err != nil {
		return fmt.Errorf("create large_sales: %v", err)
	}

	// Create small_products table
	if _, err := db.Exec(`DROP TABLE IF EXISTS small_products`); err != nil {
		return fmt.Errorf("drop small_products: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE small_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        price REAL NOT NULL,
        in_stock BOOLEAN DEFAULT 1,
        supplier_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`); err != nil {
		return fmt.Errorf("create small_products: %v", err)
	}

	// Seed large_sales with sample data
	if err := seedLargeSalesData(db); err != nil {
		return fmt.Errorf("seed large_sales: %v", err)
	}

	// Seed small_products with sample data
	if err := seedSmallProductsData(db); err != nil {
		return fmt.Errorf("seed small_products: %v", err)
	}

	return nil
}

// seedLargeSalesData populates the large_sales table
func seedLargeSalesData(db *sql.DB) error {
	log.Println("Seeding large_sales table with 50,000 records...")

	regions := []string{"North America", "Europe", "Asia", "South America", "Africa", "Oceania"}
	categories := []string{"Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Beauty"}
	paymentMethods := []string{"Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash"}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
        INSERT INTO large_sales (customer_id, order_date, amount, region, product_category, sales_rep_id, payment_method)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		return fmt.Errorf("prepare statement: %v", err)
	}
	defer stmt.Close()

	recordCount := 50000
	for i := 0; i < recordCount; i++ {
		if i%5000 == 0 && i > 0 {
			log.Printf("Inserted %d/%d large_sales records...", i, recordCount)
		}

		customerID := rand.Intn(10000) + 1
		orderDate := time.Now().AddDate(0, 0, -rand.Intn(365)).Format("2006-01-02")

		// Realistic amount distribution
		var amount float64
		if rand.Float64() < 0.7 {
			amount = float64(rand.Intn(500)) + 10.0 // $10-$510
		} else if rand.Float64() < 0.9 {
			amount = float64(rand.Intn(2000)) + 500.0 // $500-$2500
		} else {
			amount = float64(rand.Intn(5000)) + 2000.0 // $2000-$7000
		}

		region := regions[rand.Intn(len(regions))]
		category := categories[rand.Intn(len(categories))]
		salesRepID := rand.Intn(100) + 1
		paymentMethod := paymentMethods[rand.Intn(len(paymentMethods))]

		_, err = stmt.Exec(customerID, orderDate, amount, region, category, salesRepID, paymentMethod)
		if err != nil {
			return fmt.Errorf("insert record %d: %v", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %v", err)
	}

	log.Printf("Successfully seeded large_sales with %d records", recordCount)
	return nil
}

// seedSmallProductsData populates the small_products table
func seedSmallProductsData(db *sql.DB) error {
	log.Println("Seeding small_products table with 1,000 records...")

	products := []string{
		"Wireless Headphones", "Bluetooth Speaker", "Phone Case", "Laptop Stand",
		"Coffee Mug", "Water Bottle", "Notebook", "Pen Set", "Mouse Pad",
		"USB Cable", "Power Bank", "Desk Lamp", "Phone Charger", "Backpack",
		"T-Shirt", "Jeans", "Sneakers", "Watch", "Sunglasses", "Hat",
	}

	categories := []string{"Electronics", "Office Supplies", "Clothing", "Accessories"}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
        INSERT INTO small_products (product_name, category, price, in_stock, supplier_id)
        VALUES (?, ?, ?, ?, ?)
    `)
	if err != nil {
		return fmt.Errorf("prepare statement: %v", err)
	}
	defer stmt.Close()

	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		productName := fmt.Sprintf("%s #%d", products[rand.Intn(len(products))], rand.Intn(1000))
		category := categories[rand.Intn(len(categories))]
		price := float64(rand.Intn(500)) + 5.0 // $5 to $505
		inStock := rand.Float64() > 0.1        // 90% in stock
		supplierID := rand.Intn(50) + 1

		_, err = stmt.Exec(productName, category, price, inStock, supplierID)
		if err != nil {
			return fmt.Errorf("insert product %d: %v", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %v", err)
	}

	log.Printf("Successfully seeded small_products with %d records", recordCount)
	return nil
}
