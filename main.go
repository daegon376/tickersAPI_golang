package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dbFilename string = "tickers.db"
	sourceURL string = "https://api.blockchain.com/v3/exchange/tickers"
	tickersAmount int = 102
	updatePeriod = 30 * time.Second
	port string = ":8090"
)

type Ticker struct {
	Symbol    string  `json:"symbol,omitempty"`
	Price     float64 `json:"price_24h,omitempty"`
	Volume    float64 `json:"volume_24h,omitempty"`
	LastTrade float64 `json:"last_trade_price,omitempty"`
}


// DataBase part

// Initialise DB
func prepareDB() {
	os.Remove(dbFilename)
	log.Println("Creating new database:", dbFilename)
	file, err := os.Create(dbFilename)
	if err != nil {
		log.Fatal(err.Error())
	}
	file.Close()
	log.Println(dbFilename, "created!")

	tickersDatabase, _ := sql.Open("sqlite3", dbFilename)
	defer tickersDatabase.Close()

	createTable(tickersDatabase)
	insertTickers(tickersDatabase, getData())
}

func createTable(db *sql.DB) {
	createSQL := `CREATE TABLE IF NOT EXISTS tickers (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		symbol TEXT NOT NULL UNIQUE,
		price REAL NOT NULL,
		volume REAL NOT NULL,
		last_trade REAL NOT NULL
		);`

	statement, err := db.Prepare(createSQL)
	if err != nil {
		log.Fatal(err.Error())
	}
	statement.Exec()
	log.Println("tickers table created")
}

func insertTickers(db *sql.DB, tickerArr [tickersAmount]Ticker) {
	insertSQL := `INSERT INTO tickers
		(symbol, price, volume, last_trade)
	VALUES
		(?,?,?,?);`

	statement, err := db.Prepare(insertSQL)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for _, ticker := range tickerArr {
		_, err = statement.Exec(ticker.Symbol, ticker.Price, ticker.Volume, ticker.LastTrade)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}
	log.Println("First data insertion succeed")
}

// Update DB
func updateTickers(db *sql.DB, tickerArr [tickersAmount]Ticker)  {
	updateSQL := `
    UPDATE tickers
    SET price = ?,
        volume = ?,
        last_trade = ?
    WHERE
        symbol = ?`

	statement, err := db.Prepare(updateSQL)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for _, ticker := range tickerArr {
		_, err = statement.Exec(ticker.Price, ticker.Volume, ticker.LastTrade, ticker.Symbol)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}
}

func getData() [tickersAmount]Ticker{
	resp, err := http.Get(sourceURL)
	if err != nil {
		log.Println("Error while trying to det data from URL:")
		log.Println(sourceURL)
		log.Println(err.Error())
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if !json.Valid(data) {
		log.Println("Invalid json!")
	}

	var allTickers [tickersAmount]Ticker
	err = json.Unmarshal(data, &allTickers)
	if err != nil {
		log.Println(err.Error())
	}

	return allTickers
}

func updateDB() {
	for {
		time.Sleep(updatePeriod)
		log.Println("Database updating started...")

		tickersDatabase, _ := sql.Open("sqlite3", dbFilename)

		updateTickers(tickersDatabase, getData())
		tickersDatabase.Close()

		log.Println("Database updated successfully!")
	}
}

// Get data from DB
func selectAll(db *sql.DB) (tickerArr []Ticker) {
	row, err := db.Query("SELECT * FROM tickers")
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer row.Close()
	for row.Next() {
		var id int
		var symbol string
		var price, volume, lastTrade float64
		row.Scan(&id, &symbol, &price, &volume, &lastTrade)
		currentTicker := Ticker{symbol, price, volume, lastTrade}
		tickerArr = append(tickerArr, currentTicker)
	}

	if len(tickerArr) == tickersAmount {
		return tickerArr
	} else {
		log.Println("Error while extracting tickers from Database")
		return nil
	}
}



// Server part

func generalResponse(wrtr http.ResponseWriter, req *http.Request) {

	tickersDatabase, _ := sql.Open("sqlite3", dbFilename)
	defer tickersDatabase.Close()
	selectedData := selectAll(tickersDatabase)

	type TickerData struct {
		Price float64 `json:"price"`
		Volume float64 `json:"volume"`
		LastTrade float64 `json:"last_trade"`
	}

	symbols := make(map[string]TickerData)
	for _, ticker := range selectedData {
		symbols[ticker.Symbol] = TickerData{ticker.Price, ticker.Volume, ticker.LastTrade}
	}

	//resp, _ := json.MarshalIndent(symbols, "", "	")  // nice json
	resp, _ := json.Marshal(symbols) 					// ugly json
	fmt.Fprintf(wrtr, string(resp))
}

func runServer()  {
	http.HandleFunc("/", generalResponse)
	log.Fatal(http.ListenAndServe(port, nil))
}


// Executive part

func main() {
	prepareDB()
	log.Println("Starting server at http://127.0.0.1" + port + "/")
	go runServer()
	go updateDB()

	for {
		time.Sleep(time.Hour)
	}
}
