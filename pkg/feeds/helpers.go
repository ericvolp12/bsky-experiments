package feeds

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	bloom "github.com/bits-and-blooms/bloom/v3"
)

type ErrInvalidCursor struct {
	error
}

// ParseCursor takes a cursor string and returns the postID, bloom filter, and hotness
// Cursors are formatted as follows:
// <postID>:<hotness>:<bloomFilter>
func ParseCursor(cursor string, bloomFilterMaxSize uint, bloomFilterFalsePositiveRate float64) (string, *bloom.BloomFilter, float64, error) {
	cursorPostID := ""
	cursorHotness := float64(-1)
	var bloomFilter *bloom.BloomFilter
	var err error

	if cursor != "" {
		cursorParts := strings.Split(cursor, ":")
		if len(cursorParts) != 3 {
			return cursorPostID, bloomFilter, cursorHotness, ErrInvalidCursor{fmt.Errorf("cursor is invalid (wrong number of parts)")}
		}
		cursorPostID = cursorParts[0]

		cursorHotness, err = strconv.ParseFloat(cursorParts[1], 64)
		if err != nil {
			return cursorPostID, bloomFilter, cursorHotness, ErrInvalidCursor{fmt.Errorf("cursor is invalid (failed to parse hotness)")}
		}

		// grab the bloom filter from the cursor
		filterString := cursorParts[2]

		// convert the string back to a byte slice
		filterBytes, err := base64.URLEncoding.DecodeString(filterString)
		if err != nil {
			return cursorPostID, bloomFilter, cursorHotness, ErrInvalidCursor{fmt.Errorf("cursor is invalid (failed to decode filter)")}
		}

		// unmarshal the byte slice into a bloom filter
		bloomFilter = bloom.NewWithEstimates(bloomFilterMaxSize, bloomFilterFalsePositiveRate)
		err = bloomFilter.UnmarshalBinary(filterBytes)
		if err != nil {
			return cursorPostID, bloomFilter, cursorHotness, ErrInvalidCursor{fmt.Errorf("cursor is invalid (failed to unmarshal filter)")}
		}
	}

	// if the bloom filter is nil, create a new one
	if bloomFilter == nil {
		bloomFilter = bloom.NewWithEstimates(bloomFilterMaxSize, bloomFilterFalsePositiveRate)
	}

	return cursorPostID, bloomFilter, cursorHotness, nil
}

func AssembleCursor(postID string, bloomFilter *bloom.BloomFilter, hotness float64) (string, error) {
	// marshal the bloom filter
	filterBytes, err := bloomFilter.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("failed to marshal bloom filter: %v", err)
	}

	// convert the byte slice to a string
	filterString := base64.URLEncoding.EncodeToString(filterBytes)

	// assemble the cursor
	cursor := fmt.Sprintf("%s:%f:%s", postID, hotness, filterString)

	return cursor, nil
}

// ParseTimebasedCursor takes a cursor string and returns the postID, bloom filter, and hotness
// Cursors are formatted as follows:
// <postID>:<hotness>:<bloomFilter>
func ParseTimebasedCursor(cursor string, bloomFilterMaxSize uint, bloomFilterFalsePositiveRate float64) (time.Time, *bloom.BloomFilter, float64, error) {
	cursorCreatedAt := time.Now()
	cursorHotness := float64(-1)
	var bloomFilter *bloom.BloomFilter

	if cursor != "" {
		cursorParts := strings.Split(cursor, ":")
		if len(cursorParts) != 3 {
			return cursorCreatedAt, bloomFilter, cursorHotness, ErrInvalidCursor{fmt.Errorf("cursor is invalid (wrong number of parts)")}
		}
		createdAtUnixNano, err := strconv.ParseInt(cursorParts[0], 10, 64)
		if err != nil {
			return cursorCreatedAt, bloomFilter, cursorHotness, ErrInvalidCursor{fmt.Errorf("cursor is invalid (failed to parse createdAt)")}
		}

		cursorCreatedAt = time.Unix(0, createdAtUnixNano)

		cursorHotness, err = strconv.ParseFloat(cursorParts[1], 64)
		if err != nil {
			return cursorCreatedAt, bloomFilter, cursorHotness, ErrInvalidCursor{fmt.Errorf("cursor is invalid (failed to parse hotness)")}
		}

		// grab the bloom filter from the cursor
		filterString := cursorParts[2]

		// convert the string back to a byte slice
		filterBytes, err := base64.URLEncoding.DecodeString(filterString)
		if err != nil {
			return cursorCreatedAt, bloomFilter, cursorHotness, ErrInvalidCursor{fmt.Errorf("cursor is invalid (failed to decode filter)")}
		}

		// unmarshal the byte slice into a bloom filter
		bloomFilter = bloom.NewWithEstimates(bloomFilterMaxSize, bloomFilterFalsePositiveRate)
		err = bloomFilter.UnmarshalBinary(filterBytes)
		if err != nil {
			return cursorCreatedAt, bloomFilter, cursorHotness, ErrInvalidCursor{fmt.Errorf("cursor is invalid (failed to unmarshal filter)")}
		}
	}

	// if the bloom filter is nil, create a new one
	if bloomFilter == nil {
		bloomFilter = bloom.NewWithEstimates(bloomFilterMaxSize, bloomFilterFalsePositiveRate)
	}

	return cursorCreatedAt, bloomFilter, cursorHotness, nil
}

func AssembleTimebasedCursor(createdAt time.Time, bloomFilter *bloom.BloomFilter, hotness float64) (string, error) {
	// marshal the bloom filter
	filterBytes, err := bloomFilter.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("failed to marshal bloom filter: %v", err)
	}

	// convert the byte slice to a string
	filterString := base64.URLEncoding.EncodeToString(filterBytes)

	// assemble the cursor
	cursor := fmt.Sprintf("%d:%f:%s", createdAt.UnixNano(), hotness, filterString)

	return cursor, nil
}
