package main

import (
	"fmt"
	"os"
	"strconv"

	"333/scrapper"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Println("Usage: fetch_data <function_name> <start_datetime> <end_datetime> <row_count>")
		os.Exit(1)
	}

	functionName := os.Args[1]
	startDateTime := os.Args[2]
	endDateTime := os.Args[3]
	rowCount, err := strconv.Atoi(os.Args[4])
	if err != nil {
		fmt.Println("Invalid row count:", err)
		os.Exit(1)
	}

	switch functionName {
	case "LoadForecast":
		scrapper.FetchAndSaveLoadForecast(rowCount, startDateTime, endDateTime)
	case "RT_HRL_LMPS":
		scrapper.FetchAndSaveRT_HRL_LMPS(rowCount, startDateTime, endDateTime)
	case "SolarForecast":
		scrapper.FetchAndSaveSolarForecast(rowCount, startDateTime, endDateTime)
	case "WindPowerForecast":
		scrapper.FetchAndSaveWindPowerForecast(rowCount, startDateTime, endDateTime)
	case "x":
		os.Exit(0)
	default:
		fmt.Println("Invalid function name:", functionName)
		os.Exit(1)
	}
}
