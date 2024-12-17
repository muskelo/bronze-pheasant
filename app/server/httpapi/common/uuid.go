package common

import (
	uuidp "github.com/google/uuid"
)

func IsValidUUID(u string) bool {
	_, err := uuidp.Parse(u)
	return err == nil
}
