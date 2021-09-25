package main

import (
	goLog "log"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const DbPath = "analysis.sqlite3"
const reportPath = "./reports"
const RainLimit = 0.1

func main() {
	// Setup logging
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	log.Logger = log.Output(output)

	// Setup DB logger
	dbLogger := logger.New(
		goLog.New(os.Stdout, "\r\n", goLog.LstdFlags), // io writer
		logger.Config{
			SlowThreshold: time.Second, // Slow SQL threshold
		},
	)

	// Connect to DB
	db, err := gorm.Open(sqlite.Open(DbPath), &gorm.Config{Logger: dbLogger})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to DB")
	}

	// Reports
	reportWeekVsWeekend(db, RainLimit)
}

func ternaryToString(isTrue bool, trueValue, falseValue string) string {
	if isTrue {
		return trueValue
	}
	return falseValue
}
