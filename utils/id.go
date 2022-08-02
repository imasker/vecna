package utils

import (
	gonanoid "github.com/matoous/go-nanoid/v2"
)

func GenerateID(prefix string, l ...int) string {
	id, _ := gonanoid.New(l...)
	return prefix + id
}
