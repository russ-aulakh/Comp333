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
	"strconv"
	"strings"
)

type LoadForecastItem struct {
	EvaluatedAtUTC           string `json:"evaluated_at_utc"`
	EvaluatedAtEPT           string `json:"evaluated_at_ept"`
	ForecastHourBeginningUTC string `json:"forecast_hour_beginning_utc"`
	ForecastHourBeginningEPT string `json:"forecast_hour_beginning_ept"`
	ForecastArea             string `json:"forecast_area"`
	ForecastLoadMW           int    `json:"forecast_load_mw"`
}

type LoadForecastResponse struct {
	Items []LoadForecastItem `json:"items"`
}

func FetchAndSaveLoadForecast(rowCount int, startDateTime, endDateTime string) {
	start := url.QueryEscape(startDateTime)
	end := url.QueryEscape(endDateTime)

	apiURL := fmt.Sprintf("https://api.pjm.com/api/v1/load_frcstd_hist?rowCount=%d&sort=forecast_hour_beginning_utc&order=Asc&startRow=1&isActiveMetadata=true&fields=evaluated_at_ept,evaluated_at_utc,forecast_area,forecast_hour_beginning_ept,forecast_hour_beginning_utc,forecast_load_mw&forecast_hour_beginning_ept=%sto%s", rowCount, start, end)

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

	var apiResponse LoadForecastResponse
	err = json.Unmarshal(body, &apiResponse)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	safeStart := strings.ReplaceAll(startDateTime, "/", "_")
	safeStart = strings.ReplaceAll(safeStart, ":", "_")
	safeEnd := strings.ReplaceAll(endDateTime, "/", "_")
	safeEnd = strings.ReplaceAll(safeEnd, ":", "_")

	responseDir := fmt.Sprintf("response_%s_to_%s",
		strings.ReplaceAll(startDateTime, "/", "_"),
		strings.ReplaceAll(endDateTime, "/", "_"),
	)
	err = os.MkdirAll(responseDir, os.ModePerm)
	if err != nil {
		log.Fatalf("Error creating response directory: %v", err)
	}

	// Create the file path inside the response directory
	fileName := filepath.Join(responseDir, fmt.Sprintf("load_frcstd_hist.csv"))
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"Evaluated At UTC", "Evaluated At EPT", "Forecast Hour Beginning UTC", "Forecast Hour Beginning EPT", "Forecast Area", "Forecast Load MW"}
	err = writer.Write(header)
	if err != nil {
		log.Fatalf("Error writing header: %v", err)
	}

	for _, item := range apiResponse.Items {
		record := []string{
			item.EvaluatedAtUTC,
			item.EvaluatedAtEPT,
			item.ForecastHourBeginningUTC,
			item.ForecastHourBeginningEPT,
			item.ForecastArea,
			strconv.Itoa(item.ForecastLoadMW),
		}
		err = writer.Write(record)
		if err != nil {
			log.Fatalf("Error writing record: %v", err)
		}
	}

	fmt.Printf("CSV file created: %s\n", fileName)
}
