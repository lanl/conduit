// Copyright 2026. Triad National Security, LLC. All rights reserved.

package ftacmd

import (
	proto "github.com/lanl/conduit/api"
)

// errToErrs adds the provided error and proto error to a list of FTAPathErrors
func errToErrs(err error, pErr proto.Error) *proto.FTAPluginErrors {
	errs := &proto.FTAPluginErrors{
		Errors: []*proto.FTAPathError{
			{
				ErrMessage: err.Error(),
				PErr:       pErr,
			},
		},
	}

	return errs
}
