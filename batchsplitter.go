// File: batchsplitter.go

package grnoti

type deviceBatchSplitter struct{}

var _ BatchSplitter = deviceBatchSplitter{}

// NewBatchSplitter returns a stateless BatchSplitter.
func NewBatchSplitter() BatchSplitter { return deviceBatchSplitter{} }

func (deviceBatchSplitter) Deduplicate(devices []Device) []Device {
	seen := make(map[string]struct{}, len(devices))
	out := make([]Device, 0, len(devices))
	for _, dev := range devices {
		if _, ok := seen[dev.FID]; ok {
			continue
		}
		seen[dev.FID] = struct{}{}
		out = append(out, dev)
	}
	return out
}

func (deviceBatchSplitter) Split(devices []Device, maxBatchSize int) [][]Device {
	if maxBatchSize <= 0 || len(devices) == 0 {
		return [][]Device{devices}
	}
	batches := make([][]Device, 0, (len(devices)+maxBatchSize-1)/maxBatchSize)
	for i := 0; i < len(devices); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(devices) {
			end = len(devices)
		}
		batches = append(batches, devices[i:end])
	}
	return batches
}
