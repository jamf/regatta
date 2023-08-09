// Copyright JAMF Software, LLC

package fsm

import (
	"github.com/jamf/regatta/regattapb"
)

type commandDummy struct{}

func (c commandDummy) handle(ctx *updateContext) (UpdateResult, *regattapb.CommandResult, error) {
	return ResultSuccess, &regattapb.CommandResult{Revision: ctx.index}, nil
}
