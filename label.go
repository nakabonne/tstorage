package tstorage

import (
	"sort"

	"github.com/nakabonne/tstorage/internal/encoding"
)

// Label is a time-series label.
// A label without name means "__name__".
type Label struct {
	Name  []byte
	Value []byte
}

const (
	// The maximum length of label name.
	//
	// Longer names are truncated.
	maxLabelNameLen = 256

	// The maximum length of label value.
	//
	// Longer values are truncated.
	maxLabelValueLen = 16 * 1024
)

// MarshalMetricName builds a unique bytes by encoding labels.
func MarshalMetricName(labels []Label) string {
	// Determine the bytes size in advance.
	size := 0
	sort.Slice(labels, func(i, j int) bool {
		return string(labels[i].Name) < string(labels[j].Name)
	})
	for i := range labels {
		label := &labels[i]
		if len(label.Value) == 0 {
			continue
		}
		if len(label.Name) > maxLabelNameLen {
			label.Name = label.Name[:maxLabelNameLen]
		}
		if len(label.Value) > maxLabelValueLen {
			label.Value = label.Value[:maxLabelValueLen]
		}
		if string(label.Name) == "__name__" {
			label.Name = label.Name[:0]
		}
		size += len(label.Name)
		size += len(label.Value)
		size += 4
	}
	out := make([]byte, 0, size)
	// Start building the bytes.
	for i := range labels {
		label := &labels[i]
		if len(label.Value) == 0 {
			continue
		}
		out = encoding.MarshalUint16(out, uint16(len(label.Name)))
		out = append(out, label.Name...)
		out = encoding.MarshalUint16(out, uint16(len(label.Value)))
		out = append(out, label.Value...)
	}
	return string(out)
}

// FIXME: Enable to build labels using metricName
func UnmarshalMetricName(metricName string) []Label {
	return nil
}
