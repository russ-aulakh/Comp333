package scrapper

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// WindPowerForecastItem represents a single forecast record.
type WindPowerForecastItem struct {
	EvaluatedAtUTC       string  `json:"evaluated_at_utc"`
	EvaluatedAtEPT       string  `json:"evaluated_at_ept"`
	DatetimeBeginningUTC string  `json:"datetime_beginning_utc"`
	DatetimeBeginningEPT string  `json:"datetime_beginning_ept"`
	DatetimeEndingUTC    string  `json:"datetime_ending_utc"`
	DatetimeEndingEPT    string  `json:"datetime_ending_ept"`
	WindForecastMWH      float64 `json:"wind_forecast_mwh"`
}

// WindPowerForecastResponse represents the API response structure.
type WindPowerForecastResponse struct {
	Items []WindPowerForecastItem `json:"items"`
}

// FetchAndSaveWindPowerForecast fetches data from the wind power forecast API and saves it as a CSV file.
func FetchAndSaveWindPowerForecast(rowCount int, startDateTime, endDateTime string) {

	start := url.QueryEscape(startDateTime)
	end := url.QueryEscape(endDateTime)
	// Construct the API URL
	apiURL := fmt.Sprintf(
		"https://api.pjm.com/api/v1/hourly_wind_power_forecast?rowCount=%d&sort=evaluated_at_utc&order=Desc&startRow=1&isActiveMetadata=true&fields=datetime_beginning_ept,datetime_beginning_utc,datetime_ending_ept,datetime_ending_utc,evaluated_at_ept,evaluated_at_utc,wind_forecast_mwh&evaluated_at_ept=%sto%s",
		rowCount, start, end,
	)

	// Create a new HTTP request
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	// Set headers
	req.Header.Set("Ocp-Apim-Subscription-Key", "18b56f8b0eda44efabe5d60a5270cc34")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Encoding", "gzip")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error making request: %v", err)
	}
	defer resp.Body.Close()

	// Handle gzip response
	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			log.Fatalf("Error creating gzip reader: %v", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		log.Fatalf("Error reading response body: %v", err)
	}

	// Parse the JSON response
	var apiResponse WindPowerForecastResponse
	err = json.Unmarshal(body, &apiResponse)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	// Sanitize file name
	safeStart := strings.ReplaceAll(startDateTime, "/", "_")
	safeStart = strings.ReplaceAll(safeStart, ":", "_")
	safeEnd := strings.ReplaceAll(endDateTime, "/", "_")
	safeEnd = strings.ReplaceAll(safeEnd, ":", "_")

	// Create CSV file
	responseDir := fmt.Sprintf("DataLake/Raw/hourly_wind_power_forecast")
	err = os.MkdirAll(responseDir, os.ModePerm)
	if err != nil {
		log.Fatalf("Error creating response directory: %v", err)
	}

	layout := "01/02/2006 15:04"
	startT, err := time.Parse(layout, startDateTime)
	if err != nil {
		log.Fatalf("Error parsing start date: %v", err)
	}
	endT, err := time.Parse(layout, endDateTime)
	if err != nil {
		log.Fatalf("Error parsing end date: %v", err)
	}

	startFormatted := startT.Format("01_02_2006_15_04")
	endFormatted := endT.Format("01_02_2006_15_04")

	fileName := filepath.Join(
		responseDir,
		fmt.Sprintf("%s_to_%s.csv", startFormatted, endFormatted),
	)
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	header := []string{"Evaluated At UTC", "Evaluated At EPT", "Datetime Beginning UTC", "Datetime Beginning EPT", "Datetime Ending UTC", "Datetime Ending EPT", "Wind Forecast MWH"}
	err = writer.Write(header)
	if err != nil {
		log.Fatalf("Error writing header: %v", err)
	}

	// Write CSV rows
	for _, item := range apiResponse.Items {
		record := []string{
			item.EvaluatedAtUTC,
			item.EvaluatedAtEPT,
			item.DatetimeBeginningUTC,
			item.DatetimeBeginningEPT,
			item.DatetimeEndingUTC,
			item.DatetimeEndingEPT,
			fmt.Sprintf("%.3f", item.WindForecastMWH),
		}
		err = writer.Write(record)
		if err != nil {
			log.Fatalf("Error writing record: %v", err)
		}
	}

	fmt.Printf("CSV file created: %s\n", fileName)
}
