package fsm

import (
	"github.com/wandera/regatta/proto"
)

type commandDummy struct {
	*updateContext
}

func (c commandDummy) handle() (UpdateResult, *proto.CommandResult, error) {
	return ResultSuccess, &proto.CommandResult{Revision: c.index}, nil
}
