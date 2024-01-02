// Copyright JAMF Software, LLC

package regattapb

func (req *TxnRequest) IsReadonly() bool {
	for _, op := range req.Success {
		if _, ok := op.Request.(*RequestOp_RequestRange); !ok {
			return false
		}
	}

	for _, op := range req.Failure {
		if _, ok := op.Request.(*RequestOp_RequestRange); !ok {
			return false
		}
	}
	return true
}
