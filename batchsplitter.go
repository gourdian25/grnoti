// File: batchsplitter.go

package grnoti

type tokenBatchSplitter struct{}

var _ BatchSplitter = tokenBatchSplitter{}

// NewBatchSplitter returns a stateless BatchSplitter.
func NewBatchSplitter() BatchSplitter { return tokenBatchSplitter{} }

func (tokenBatchSplitter) Deduplicate(tokens []DeviceToken) []DeviceToken {
	seen := make(map[string]struct{}, len(tokens))
	out := make([]DeviceToken, 0, len(tokens))
	for _, t := range tokens {
		if _, ok := seen[t.Token]; ok {
			continue
		}
		seen[t.Token] = struct{}{}
		out = append(out, t)
	}
	return out
}

func (tokenBatchSplitter) Split(tokens []DeviceToken, maxBatchSize int) [][]DeviceToken {
	if maxBatchSize <= 0 || len(tokens) == 0 {
		return [][]DeviceToken{tokens}
	}
	batches := make([][]DeviceToken, 0, (len(tokens)+maxBatchSize-1)/maxBatchSize)
	for i := 0; i < len(tokens); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(tokens) {
			end = len(tokens)
		}
		batches = append(batches, tokens[i:end])
	}
	return batches
}
