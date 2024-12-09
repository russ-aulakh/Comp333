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

type RTLMPSItem struct {
	DatetimeBeginningUTC string  `json:"datetime_beginning_utc"`
	DatetimeBeginningEPT string  `json:"datetime_beginning_ept"`
	PnodeID              int     `json:"pnode_id"`
	PnodeName            string  `json:"pnode_name"`
	Voltage              string  `json:"voltage"`
	Equipment            string  `json:"equipment"`
	Type                 string  `json:"type"`
	Zone                 string  `json:"zone"`
	SystemEnergyPriceRT  float64 `json:"system_energy_price_rt"`
	TotalLMP_RT          float64 `json:"total_lmp_rt"`
	CongestionPriceRT    float64 `json:"congestion_price_rt"`
	MarginalLossPriceRT  float64 `json:"marginal_loss_price_rt"`
	RowIsCurrent         bool    `json:"row_is_current"`
	VersionNbr           int     `json:"version_nbr"`
}

type RTLMPSResponse struct {
	Items []RTLMPSItem `json:"items"`
}

func FetchAndSaveRT_HRL_LMPS(rowCount int, startDateTime, endDateTime string) {
	// Encode query parameters
	start := url.QueryEscape(startDateTime)
	end := url.QueryEscape(endDateTime)

	// Construct the API URL
	apiURL := fmt.Sprintf("https://api.pjm.com/api/v1/rt_hrl_lmps?rowCount=%d&sort=datetime_beginning_ept&order=Asc&startRow=1&isActiveMetadata=true&fields=congestion_price_rt,datetime_beginning_ept,datetime_beginning_utc,equipment,marginal_loss_price_rt,pnode_id,pnode_name,row_is_current,system_energy_price_rt,total_lmp_rt,type,version_nbr,voltage,zone&datetime_beginning_ept=%sto%s", rowCount, start, end)

	// Create HTTP client and request
	client := &http.Client{}
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	// Set headers
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Ocp-Apim-Subscription-Key", "18b56f8b0eda44efabe5d60a5270cc34")

	// Execute request
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error making request: %v", err)
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	var gzipReader *gzip.Reader
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err = gzip.NewReader(resp.Body)
		if err != nil {
			log.Fatalf("Error decompressing response: %v", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		log.Fatalf("Error reading response body: %v", err)
	}

	// Parse JSON response
	var apiResponse RTLMPSResponse
	err = json.Unmarshal(body, &apiResponse)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	// Sanitize file name
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

	fileName := filepath.Join(responseDir, fmt.Sprintf("rt_hrl_lmps_%s_to_%s.csv", safeStart, safeEnd))
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"Datetime Beginning UTC", "Datetime Beginning EPT", "Pnode ID", "Pnode Name", "Voltage", "Equipment", "Type", "Zone", "System Energy Price RT", "Total LMP RT", "Congestion Price RT", "Marginal Loss Price RT", "Row Is Current", "Version Number"}
	err = writer.Write(header)
	if err != nil {
		log.Fatalf("Error writing header: %v", err)
	}

	for _, item := range apiResponse.Items {
		record := []string{
			item.DatetimeBeginningUTC,
			item.DatetimeBeginningEPT,
			strconv.Itoa(item.PnodeID),
			item.PnodeName,
			item.Voltage,
			item.Equipment,
			item.Type,
			item.Zone,
			fmt.Sprintf("%.6f", item.SystemEnergyPriceRT),
			fmt.Sprintf("%.6f", item.TotalLMP_RT),
			fmt.Sprintf("%.6f", item.CongestionPriceRT),
			fmt.Sprintf("%.6f", item.MarginalLossPriceRT),
			strconv.FormatBool(item.RowIsCurrent),
			strconv.Itoa(item.VersionNbr),
		}
		err = writer.Write(record)
		if err != nil {
			log.Fatalf("Error writing record: %v", err)
		}
	}

	fmt.Printf("CSV file created: %s\n", fileName)
}
