package util

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
)

// InsertMentions replaces all {handle:n} mentions with the appropriate handle
func InsertMentions(text string, dids []string, handles []string, facets []*appbsky.RichtextFacet) (string, []*appbsky.RichtextFacet, error) {
	if len(dids) != len(handles) {
		return "", nil, fmt.Errorf("length of DIDs and handles should be the same")
	}

	placeholderPattern := regexp.MustCompile(`\{handle:(\d+)\}`)
	matches := placeholderPattern.FindAllStringSubmatch(text, -1)

	for _, match := range matches {
		if len(match) != 2 {
			continue
		}

		index, err := strconv.Atoi(match[1])
		if err != nil || index >= len(handles) {
			continue
		}

		// Truncate if necessary
		truncatedHandle := handles[index]
		if len(truncatedHandle) > 40 {
			truncatedHandle = truncatedHandle[:37] + "..."
		}

		// Create the facet
		startIdx := int64(strings.Index(text, match[0]))
		endIdx := startIdx + int64(len(truncatedHandle)) + 1

		facet := &appbsky.RichtextFacet{
			Features: []*appbsky.RichtextFacet_Features_Elem{{
				RichtextFacet_Mention: &appbsky.RichtextFacet_Mention{
					Did: dids[index],
				},
			}},
			Index: &appbsky.RichtextFacet_ByteSlice{
				ByteStart: startIdx,
				ByteEnd:   endIdx,
			},
		}

		facets = append(facets, facet)

		// Replace the placeholder with the handle in the text
		text = strings.Replace(text, match[0], "@"+truncatedHandle, 1)
	}

	return text, facets, nil
}

// InsertLinks replaces all {link:n} links with an abbreviated link and adds a facet
func InsertLinks(text string, urls []string, texts []string, facets []*appbsky.RichtextFacet) (string, []*appbsky.RichtextFacet, error) {
	placeholderPattern := regexp.MustCompile(`\{link:(\d+)\}`)
	matches := placeholderPattern.FindAllStringSubmatch(text, -1)

	for _, match := range matches {
		if len(match) != 2 {
			continue
		}

		index, err := strconv.Atoi(match[1])
		if err != nil || index >= len(urls) {
			continue
		}

		// Truncate if necessary
		truncatedText := texts[index]
		if len(truncatedText) > 25 {
			truncatedText = truncatedText[:22] + "..."
		}

		// Create the facet
		startIdx := int64(strings.Index(text, match[0]))
		endIdx := startIdx + int64(len(truncatedText)) + 1

		facet := &appbsky.RichtextFacet{
			Features: []*appbsky.RichtextFacet_Features_Elem{{
				RichtextFacet_Link: &appbsky.RichtextFacet_Link{
					Uri: urls[index],
				},
			}},
			Index: &appbsky.RichtextFacet_ByteSlice{
				ByteStart: startIdx,
				ByteEnd:   endIdx,
			},
		}

		facets = append(facets, facet)

		// Replace the placeholder with the truncated link in the text
		text = strings.Replace(text, match[0], truncatedText, 1)
	}

	return text, facets, nil
}
