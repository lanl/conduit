// Copyright 2026. Triad National Security, LLC. All rights reserved.

package ftacmd

import (
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
)

// errToErrs adds the provided error and proto error to a list of FTAPathErrors
func errToErrs(err error, pErr proto.Error) plugin.PluginErrors {
	errs := plugin.PluginErrors{
		Errors: []*plugin.FTAPathError{
			{
				ErrMessage: err,
				PErr:       pErr,
			},
		},
	}

	return errs
}
