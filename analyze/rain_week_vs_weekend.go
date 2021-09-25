package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
)

func reportWeekVsWeekend(db *gorm.DB, rainLimitInMm float64) {
	// Fetch reports
	wholeDay := fetchDataWeekVsWeekend(db, rainLimitInMm, false)
	dayTime := fetchDataWeekVsWeekend(db, rainLimitInMm, true)

	// Merge reports
	reports := make([]*WeekVsWeekendReport, 0, len(wholeDay))
	for year, agg := range wholeDay {
		report := &WeekVsWeekendReport{
			Year:          year,
			WholeDay:      agg,
			DuringDaytime: dayTime[year],
		}
		reports = append(reports, report)
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].Year < reports[j].Year })

	// Convert to CSV
	csvReport := make([][]string, 1, len(reports)+1)
	csvReport[0] = []string{
		"jaar",
		"kans_op_regen_totaal",
		"kans_op_regen_week",
		"kans_op_regen_weekend",
		"meer_kans_op_regen_weekend",
		"dag_kans_op_regen_totaal",
		"dag_kans_op_regen_week",
		"dag_kans_op_regen_weekend",
		"dag_meer_kans_op_regen_weekend",
	}
	for _, report := range reports {
		csvReport = append(csvReport, report.ToCsv())
	}

	// Create CSV file
	csvPath := filepath.Join(reportPath, "week_vs_weekday.csv")
	csvFile, err := os.Create(csvPath)
	if err != nil {
		log.Fatal().Str("path", csvPath).Err(err).Msg("Failed to create CSV file")
	}
	csvWriter := csv.NewWriter(csvFile)
	err = csvWriter.WriteAll(csvReport)
	if err != nil {
		log.Fatal().Str("path", csvPath).Err(err).Msg("Failed to write to CSV file")
	}
}

type WeekVsWeekendReport struct {
	Year          int
	WholeDay      *WeekVsWeekendYearAgg
	DuringDaytime *WeekVsWeekendYearAgg
}

func (report WeekVsWeekendReport) ToCsv() []string {
	return []string{
		strconv.Itoa(report.Year),
		strconv.FormatFloat(report.WholeDay.ChanceOfRain, 'f', 2, 64),
		strconv.FormatFloat(report.WholeDay.ChanceOfRainDuringWeek, 'f', 2, 64),
		strconv.FormatFloat(report.WholeDay.ChanceOfRainDuringWeekend, 'f', 2, 64),
		ternaryToString(report.WholeDay.ChanceOfRainDuringWeekend > report.WholeDay.ChanceOfRainDuringWeek, "ja", "nee"),
		strconv.FormatFloat(report.DuringDaytime.ChanceOfRain, 'f', 2, 64),
		strconv.FormatFloat(report.DuringDaytime.ChanceOfRainDuringWeek, 'f', 2, 64),
		strconv.FormatFloat(report.DuringDaytime.ChanceOfRainDuringWeekend, 'f', 2, 64),
		ternaryToString(report.DuringDaytime.ChanceOfRainDuringWeekend > report.DuringDaytime.ChanceOfRainDuringWeek, "ja", "nee"),
	}
}

type WeekVsWeekendYearAgg struct {
	TotalDays                 int
	TotalWetDays              int
	WetDaysWeek               int
	TotalDaysWeek             int
	WetDaysWeekend            int
	TotalDaysWeekend          int
	ChanceOfRain              float64
	ChanceOfRainDuringWeek    float64
	ChanceOfRainDuringWeekend float64
	WeekendMoreWet            bool
}

func fetchDataWeekVsWeekend(db *gorm.DB, rainLimitInMm float64, onlyDuringDaytime bool) map[int]*WeekVsWeekendYearAgg {
	type Row struct {
		YearWeekday  string
		Rained       bool
		NumberOfDays int
	}

	// Fetch data
	data := []*Row{}
	queryRain := db.Table(`rain`)
	if onlyDuringDaytime {
		queryRain = queryRain.Select(`timestamp, CASE WHEN CAST(strftime('%H',timestamp, 'localtime') AS INTEGER) < 7 OR CAST(strftime('%H',timestamp, 'localtime') AS INTEGER) >= 23 THEN 0 ELSE value END value`)
	} else {
		queryRain = queryRain.Select(`timestamp, value`)
	}
	queryDayAgg := db.
		Select(`DATE(timestamp) AS date, SUM(value) AS value, SUM(value) > ? AS rained`, rainLimitInMm).
		Table(`(?)`, queryRain).
		Group(`DATE(timestamp)`)
	err := db.
		Select(`strftime('%Y-%w',date) as year_weekday, rained, COUNT(*) as number_of_days`).
		Table(`(?)`, queryDayAgg).
		Group(`strftime('%Y-%w',date)`).Group(`rained`).
		Scan(&data).
		Error
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to fetch week vs weekend data")
	}

	// Derive remaining data
	report := map[int]*WeekVsWeekendYearAgg{}
	for _, row := range data {
		// Extract date parts
		dateParts := strings.Split(row.YearWeekday, "-")
		if len(dateParts) != 2 {
			log.Fatal().Str("date", row.YearWeekday).Msg("Format year-weekday expected")
		}
		year, err := strconv.Atoi(dateParts[0])
		if err != nil {
			log.Fatal().Str("year", dateParts[0]).Msg("Failed to parse year to int")
		}
		weekdayInt, err := strconv.Atoi(dateParts[1])
		if err != nil {
			log.Fatal().Str("weekday", dateParts[1]).Msg("Failed to parse weekday to int")
		}
		weekday := time.Weekday(weekdayInt)

		// Complete data
		agg, ok := report[year]
		if !ok {
			agg = new(WeekVsWeekendYearAgg)
		}
		if weekday == time.Saturday || weekday == time.Sunday {
			if row.Rained {
				agg.WetDaysWeekend += row.NumberOfDays
			}
			agg.TotalDaysWeekend += row.NumberOfDays
			agg.ChanceOfRainDuringWeekend = float64(agg.WetDaysWeekend) / float64(agg.TotalDaysWeekend)
		} else {
			if row.Rained {
				agg.WetDaysWeek += row.NumberOfDays
			}
			agg.TotalDaysWeek += row.NumberOfDays
			agg.ChanceOfRainDuringWeek = float64(agg.WetDaysWeek) / float64(agg.TotalDaysWeek)
		}
		agg.TotalDays = agg.TotalDaysWeek + agg.TotalDaysWeekend
		agg.TotalWetDays = agg.WetDaysWeek + agg.WetDaysWeekend
		agg.ChanceOfRain = float64(agg.TotalWetDays) / float64(agg.TotalDays)
		agg.WeekendMoreWet = agg.ChanceOfRainDuringWeekend > agg.ChanceOfRainDuringWeek
		if !ok {
			report[year] = agg
		}
	}
	return report
}
