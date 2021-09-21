package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const sunriseSunsetUrl = `https://www.astro.oma.be/GENERAL/INFO/nzon/zon_%d.html`
const dataPath = `./data/sunrise_sunset`
const rowColumnsCount = 6
const timeLocation = "Europe/Brussels"
const dateFormat = `02 01 2006`
const timeFormat = `15:04`
const fromYear = 2011
const toYear = 2022

type Row struct {
	Date         time.Time
	SunriseStart time.Time
	SunriseEnd   time.Time
	SunsetStart  time.Time
	SunsetEnd    time.Time
	Duration     time.Duration // Duration of sunrise and sunset in minutes
}

type Rows []*Row

func (rows Rows) ToCsv() [][]string {
	output := make([][]string, 0, len(rows)+1)
	output = append(output, []string{"date", "sunrise_start", "sunrise_end", "sunset_start", "sunset_end", "duration_minutes"})
	for _, row := range rows {
		output = append(output, []string{
			row.Date.Format(time.RFC3339),
			row.SunriseStart.Format(time.RFC3339),
			row.SunriseEnd.Format(time.RFC3339),
			row.SunsetStart.Format(time.RFC3339),
			row.SunsetEnd.Format(time.RFC3339),
			strconv.Itoa(int(row.Duration.Minutes())),
		})
	}
	return output
}

func main() {
	// Setup logging
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	log.Logger = log.Output(output)

	for i := fromYear; i <= toYear; i++ {
		// Log progress
		log.Info().Msgf("Processing year %d ...", i)

		// Get query
		yearUrl := fmt.Sprintf(sunriseSunsetUrl, i)
		query, err := queryFromUrl(yearUrl)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get query from URL")
		}

		// Parse page
		rows, err := rowsFromQuery(query)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get rows from query")
		}

		// Create CSV file
		csvPath := filepath.Join(dataPath, fmt.Sprintf("sun_%d.csv", i))
		csvFile, err := os.Create(csvPath)
		if err != nil {
			log.Fatal().Str("path", csvPath).Err(err).Msg("Failed to create CSV file")
		}
		csvWriter := csv.NewWriter(csvFile)
		err = csvWriter.WriteAll(rows.ToCsv())
		if err != nil {
			log.Fatal().Str("path", csvPath).Err(err).Msg("Failed to write to CSV file")
		}
	}
}

// Based on https://github.com/PuerkitoBio/goquery
func queryFromUrl(url string) (*goquery.Document, error) {
	// Fetch the HTML page.
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		// Extract body
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}

		// Log body
		log.Error().Bytes("body", body).Msg("HTTP request returned an error response")
		return nil, err
	}

	// Load the HTML document
	return goquery.NewDocumentFromReader(res.Body)
}

func rowsFromQuery(query *goquery.Document) (Rows, error) {
	// Init
	selection := query.Find("tr")
	rows := make(Rows, 0, selection.Length()/rowColumnsCount)
	location, err := time.LoadLocation(timeLocation)
	if err != nil {
		return nil, err
	}

	// Process
	for i := range selection.Nodes {
		// Extract columns
		htmlRow := selection.Eq(i)
		htmlCols := htmlRow.Find("td")
		if htmlCols.Length() != rowColumnsCount {
			continue
		}

		// Parse row - Date
		var row Row
		htmlDate := strings.TrimSpace(htmlCols.Eq(0).Text())
		row.Date, err = time.ParseInLocation(dateFormat, htmlDate, location)
		if err != nil {
			logErrorOnSelection(err, htmlRow, "Failed to parse date of row")
			return nil, err
		}

		// Parse row - Sunrise start
		htmlSunriseStart := strings.TrimSpace(htmlCols.Eq(4).Text())
		row.SunriseStart, err = parseTimeOnDate(htmlSunriseStart, row.Date)
		if err != nil {
			logErrorOnSelection(err, htmlRow, "Failed to parse sunrise start of row")
			return nil, err
		}

		// Parse row - Sunrise end
		htmlSunriseEnd := strings.TrimSpace(htmlCols.Eq(1).Text())
		row.SunriseEnd, err = parseTimeOnDate(htmlSunriseEnd, row.Date)
		if err != nil {
			logErrorOnSelection(err, htmlRow, "Failed to parse sunrise end of row")
			return nil, err
		}

		// Parse row - Sunset start
		htmlSunsetStart := strings.TrimSpace(htmlCols.Eq(5).Text())
		row.SunsetStart, err = parseTimeOnDate(htmlSunsetStart, row.Date)
		if err != nil {
			logErrorOnSelection(err, htmlRow, "Failed to parse sunset start of row")
			return nil, err
		}

		// Parse row - Sunset end
		htmlSunsetEnd := strings.TrimSpace(htmlCols.Eq(2).Text())
		row.SunsetEnd, err = parseTimeOnDate(htmlSunsetEnd, row.Date)
		if err != nil {
			logErrorOnSelection(err, htmlRow, "Failed to parse sunset end of row")
			return nil, err
		}

		// Parse row - Duration
		htmlDuration := strings.TrimSpace(htmlCols.Eq(3).Text())
		durationMins, err := parseInt(htmlDuration)
		if err != nil {
			logErrorOnSelection(err, htmlRow, "Failed to parse duration of row")
			return nil, err
		}
		if durationMins < 1 || durationMins > 120 {
			err := fmt.Errorf("duration of %d outside allowed range of 1 to 120 minutes", durationMins)
			log.Error().Err(err).Msg("Failed to parse duration")
			return nil, err
		}
		row.Duration = time.Duration(durationMins) * time.Minute

		// Append result
		rows = append(rows, &row)
	}
	return rows, nil
}

func logErrorOnSelection(err error, selection *goquery.Selection, msg string) {
	html, htmlErr := selection.Html()
	if htmlErr != nil {
		log.Error().Str("selection", html).Errs("errors", []error{htmlErr, err}).Msg(msg)
	} else {
		log.Error().Str("selection", html).Err(err).Msg(msg)
	}
}

func parseTimeOnDate(timeString string, date time.Time) (time.Time, error) {
	trimmedTime := strings.TrimSpace(timeString)
	timePart, err := time.Parse(timeFormat, trimmedTime)
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(date.Year(), date.Month(), date.Day(), timePart.Hour(), timePart.Minute(), 0, 0, date.Location()), nil
}

// parseInt drops all non-numeric characters and tries
// to parse remainder as an integer.
func parseInt(integer string) (int, error) {
	var cleaned strings.Builder
	for _, char := range integer {
		if unicode.IsNumber(char) {
			cleaned.WriteRune(char)
		}
	}
	return strconv.Atoi(cleaned.String())
}
