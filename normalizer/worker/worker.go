package worker

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProcessRecord(ctx context.Context, rec *kgo.Record) error {
	var err error
	return err
}
