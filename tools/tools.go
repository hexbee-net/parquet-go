// +build tools

package tools

import (
	_ "github.com/dvyukov/go-fuzz"
	_ "github.com/dvyukov/go-fuzz-corpus"
	_ "github.com/gojuno/minimock/v3/cmd/minimock"
)

// This file imports packages that are used when running go generate, or used
// during the development process but not otherwise depended on by built code.
