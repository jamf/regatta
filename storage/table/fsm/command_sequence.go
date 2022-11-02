// Copyright JAMF Software, LLC

package fsm

import (
	"github.com/wandera/regatta/proto"
)

type commandSequence struct {
	*proto.Command
}

func (c commandSequence) handle(ctx *updateContext) (UpdateResult, *proto.CommandResult, error) {
	res := &proto.CommandResult{Revision: ctx.index}
	for _, cmd := range c.Sequence {
		_, cmdRes, err := wrapCommand(cmd).handle(ctx)
		if err != nil {
			return ResultFailure, nil, err
		}
		res.Responses = append(res.Responses, cmdRes.Responses...)
	}
	return ResultSuccess, res, nil
}
