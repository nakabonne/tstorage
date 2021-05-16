package tstorage

import (
	"sort"

	"github.com/nakabonne/tstorage/internal/encoding"
)

// Label is a time-series label.
type Label struct {
	Name  string
	Value string
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

// marshalMetricName builds a unique bytes by encoding labels.
func marshalMetricName(labels []Label) string {
	// Determine the bytes size in advance.
	size := 0
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
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
//   Or, think about another way to flush in-memory data to disk
func unmarshalMetricName(metricName string) []Label {
	return nil
}
