package followers

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type ErrInvalidCursor struct {
	error
}

// ParseCursor takes a cursor string and returns the created_at, actor_did, and rkey
// <created_at>:<actor_did>:<rkey>
func ParseCursor(cursor string) (time.Time, string, string, error) {
	var err error
	createdAt := time.Now()
	actorDID := ""
	rkey := ""

	if cursor != "" {
		cursorParts := strings.Split(cursor, "|")
		if len(cursorParts) != 3 {
			return createdAt, actorDID, rkey, ErrInvalidCursor{fmt.Errorf("cursor is invalid (wrong number of parts)")}
		}
		createdAtStr := cursorParts[0]

		createdAtUnixMili := int64(0)
		createdAtUnixMili, err = strconv.ParseInt(createdAtStr, 10, 64)
		if err != nil {
			return createdAt, actorDID, rkey, ErrInvalidCursor{fmt.Errorf("cursor is invalid (failed to parse createdAt)")}
		}

		createdAt = time.UnixMilli(createdAtUnixMili)

		actorDID = cursorParts[1]
		rkey = cursorParts[2]
	}

	return createdAt, actorDID, rkey, nil
}

func AssembleCursor(createdAt time.Time, actorDID string, rkey string) string {
	return fmt.Sprintf("%d|%s|%s", createdAt.UnixMilli(), actorDID, rkey)
}
