// Copyright JAMF Software, LLC

package fsm

import (
	"github.com/jamf/regatta/proto"
)

type commandDummy struct{}

func (c commandDummy) handle(ctx *updateContext) (UpdateResult, *proto.CommandResult, error) {
	return ResultSuccess, &proto.CommandResult{Revision: ctx.index}, nil
}
