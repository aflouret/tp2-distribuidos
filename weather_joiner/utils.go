package main

import "time"

func getPreviousDate(dateString string) (string, error) {
	date, err := time.Parse("2006-01-02", dateString)
	if err != nil {
		return "", err
	}
	previousDate := date.AddDate(0, 0, -1)

	return previousDate.Format("2006-01-02"), nil
}
