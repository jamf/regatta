// Copyright JAMF Software, LLC

package gzip

import (
	gz "compress/gzip"

	"google.golang.org/grpc/encoding/gzip"
)

func init() {
	err := gzip.SetLevel(gz.BestSpeed)
	if err != nil {
		panic(err)
	}
}
