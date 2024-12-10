package main

import "333/scrapper"

func main() {
	rowCount := 1000
	startDateTime := "7/2/2024 00:00"
	endDateTime := "9/7/2024 23:59"

	scrapper.FetchAndSaveLoadForecast(rowCount, startDateTime, endDateTime)
	scrapper.FetchAndSaveRT_HRL_LMPS(rowCount, startDateTime, endDateTime)
	scrapper.FetchAndSaveSolarForecast(rowCount, startDateTime, endDateTime)
	scrapper.FetchAndSaveWindPowerForecast(rowCount, startDateTime, endDateTime)

}
