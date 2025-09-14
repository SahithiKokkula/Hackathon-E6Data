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
    if dbPath == "" { dbPath = "aqe.sqlite" }
    db, err := sql.Open("sqlite", dbPath)
    if err != nil { log.Fatalf("open db: %v", err) }
    defer db.Close()

    if _, err := db.Exec(`DROP TABLE IF EXISTS purchases`); err != nil { log.Fatalf("drop: %v", err) }
    if _, err := db.Exec(`CREATE TABLE purchases (
        id INTEGER PRIMARY KEY,
        dt TEXT,
        country TEXT,
        amount REAL
    )`); err != nil { log.Fatalf("create: %v", err) }

    rand.Seed(42)
    countries := []string{"US","IN","DE","FR","GB","BR","CA","AU","JP","MX"}
    tx, _ := db.Begin()
    stmt, _ := tx.Prepare("INSERT INTO purchases(dt,country,amount) VALUES (?,?,?)")
    defer stmt.Close()

    n := 200000 // 200k rows for quick demo
    start := time.Date(2024,1,1,0,0,0,0,time.UTC)
    for i := 0; i < n; i++ {
        d := start.Add(time.Duration(rand.Intn(365*24)) * time.Hour)
        c := countries[rand.Intn(len(countries))]
        // amount heavy-tail
        amt := 10 + rand.ExpFloat64()*50
        if _, err := stmt.Exec(d.Format(time.RFC3339), c, amt); err != nil { log.Fatalf("insert: %v", err) }
        if i%10000 == 0 { fmt.Printf("inserted %d\n", i) }
    }
    if err := tx.Commit(); err != nil { log.Fatalf("commit: %v", err) }
    fmt.Println("Seed done.")
}
