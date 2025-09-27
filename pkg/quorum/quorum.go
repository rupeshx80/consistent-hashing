//

package quorum

import (
	"fmt"
)

type QuorumConfig struct {
	N int 
	R int 
	W int 
}

func (qc *QuorumConfig) GetConsistencyLevel() string {
	if qc.R+qc.W > qc.N {
		return "STRONG" 
	}
	return "EVENTUAL" 
}

func (qc *QuorumConfig) IsSloppyQuorum() bool {
	return qc.R+qc.W <= qc.N
}