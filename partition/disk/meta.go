package disk

const metaFileName = "meta.json"

// meta is a mapper for a meta file, which is put for each partition.
type meta struct {
	MinTimestamp  int64 `json:"minTimestamp"`
	MaxTimestamp  int64 `json:"maxTimestamp"`
	NumDatapoints int   `json:"numDatapoints"`
}
