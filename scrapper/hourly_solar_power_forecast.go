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

type SolarForecastItem struct {
	EvaluatedAtUTC       string  `json:"evaluated_at_utc"`
	EvaluatedAtEPT       string  `json:"evaluated_at_ept"`
	DatetimeBeginningUTC string  `json:"datetime_beginning_utc"`
	DatetimeBeginningEPT string  `json:"datetime_beginning_ept"`
	DatetimeEndingUTC    string  `json:"datetime_ending_utc"`
	DatetimeEndingEPT    string  `json:"datetime_ending_ept"`
	SolarForecastMWH     float64 `json:"solar_forecast_mwh"`
	SolarForecastBTMMWH  float64 `json:"solar_forecast_btm_mwh"`
}

type SolarForecastResponse struct {
	Items []SolarForecastItem `json:"items"`
}

func FetchAndSaveSolarForecast(rowCount int, startDateTime, endDateTime string) {
	start := url.QueryEscape(startDateTime)
	end := url.QueryEscape(endDateTime)

	apiURL := fmt.Sprintf("https://api.pjm.com/api/v1/hourly_solar_power_forecast?rowCount=%d&sort=evaluated_at_utc&order=Desc&startRow=1&isActiveMetadata=true&fields=datetime_beginning_ept,datetime_beginning_utc,datetime_ending_ept,datetime_ending_utc,evaluated_at_ept,evaluated_at_utc,solar_forecast_btm_mwh,solar_forecast_mwh&evaluated_at_ept=%sto%s", rowCount, start, end)
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	req.Header.Set("Ocp-Apim-Subscription-Key", "18b56f8b0eda44efabe5d60a5270cc34")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Encoding", "gzip")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("HTTP Request failed: %s\nBody: %s", resp.Status, string(body))
	}

	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			log.Fatalf("Error creating gzip reader: %v", err)
		}
		defer reader.(*gzip.Reader).Close()
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		log.Fatalf("Error reading response body: %v", err)
	}

	var apiResponse SolarForecastResponse
	err = json.Unmarshal(body, &apiResponse)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	safeStart := strings.ReplaceAll(startDateTime, "/", "_")
	safeStart = strings.ReplaceAll(safeStart, ":", "_")
	safeEnd := strings.ReplaceAll(endDateTime, "/", "_")
	safeEnd = strings.ReplaceAll(safeEnd, ":", "_")

	responseDir := fmt.Sprintf("DataLake/Raw/hourly_solar_power_forecast")
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

	header := []string{"Evaluated At UTC", "Evaluated At EPT", "Datetime Beginning UTC", "Datetime Beginning EPT", "Datetime Ending UTC", "Datetime Ending EPT", "Solar Forecast MWH", "Solar Forecast BTM MWH"}
	err = writer.Write(header)
	if err != nil {
		log.Fatalf("Error writing header: %v", err)
	}

	for _, item := range apiResponse.Items {
		record := []string{
			item.EvaluatedAtUTC,
			item.EvaluatedAtEPT,
			item.DatetimeBeginningUTC,
			item.DatetimeBeginningEPT,
			item.DatetimeEndingUTC,
			item.DatetimeEndingEPT,
			fmt.Sprintf("%.3f", item.SolarForecastMWH),
			fmt.Sprintf("%.3f", item.SolarForecastBTMMWH),
		}
		err = writer.Write(record)
		if err != nil {
			log.Fatalf("Error writing record: %v", err)
		}
	}

	fmt.Printf("CSV file created: %s\n", fileName)
}
