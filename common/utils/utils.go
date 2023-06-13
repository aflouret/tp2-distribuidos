package utils

import "strings"

func ParseBatch(batch string) (id string, city string, lines []string) {
	batchFields := strings.Split(batch, "\n")
	id = strings.Split(batchFields[0], ",")[0]
	city = strings.Split(batchFields[0], ",")[1]
	lines = strings.Split(batchFields[1], ";")
	return
}

func CreateBatch(id string, city string, lines []string) string {
	batch := id + "," + city + "\n" + strings.Join(lines, ";")
	return strings.TrimSuffix(batch, ";")
}
