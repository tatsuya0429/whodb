package spanner

import (
	"errors"

	"github.com/clidey/whodb/core/src/engine"
)

func (p *SpannerPlugin) AddStorageUnit(config *engine.PluginConfig, schema string, storageUnit string, fields map[string]string) (bool, error) {
	return false, errors.ErrUnsupported
}

func (p *SpannerPlugin) AddRow(config *engine.PluginConfig, schema string, storageUnit string, values []engine.Record) (bool, error) {
	return false, errors.ErrUnsupported
}
