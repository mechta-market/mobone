package tools

import (
	"strings"
)

func ConstructSortColumns(allowedFields []string, inputSort []string) []string {
	if allowedFields == nil || len(allowedFields) == 0 {
		return nil
	}
	if inputSort == nil || len(inputSort) == 0 {
		return nil
	}

	result := make([]string, 0, len(inputSort))
	var isDesc bool

	for _, inputV := range inputSort {
		isDesc = strings.HasPrefix(inputV, "-")
		inputV = strings.TrimLeft(inputV, "-")

		for _, allowedV := range allowedFields {
			if inputV == allowedV {
				if isDesc {
					result = append(result, inputV+" desc")
				} else {
					result = append(result, inputV)
				}
				break
			}
		}
	}

	return result
}
