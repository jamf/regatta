// Copyright JAMF Software, LLC

package fsm

import (
	"github.com/jamf/regatta/regattapb"
)

type commandSequence struct {
	*regattapb.Command
}

func (c commandSequence) handle(ctx *updateContext) (UpdateResult, *regattapb.CommandResult, error) {
	res := &regattapb.CommandResult{Revision: ctx.index}
	for _, cmd := range c.Sequence {
		_, cmdRes, err := wrapCommand(cmd).handle(ctx)
		if err != nil {
			return ResultFailure, nil, err
		}
		res.Responses = append(res.Responses, cmdRes.Responses...)
	}
	return ResultSuccess, res, nil
}
