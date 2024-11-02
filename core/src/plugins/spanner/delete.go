package spanner

import (
	"errors"

	"github.com/clidey/whodb/core/src/engine"
)

func (p *SpannerPlugin) DeleteStorageUnit(config *engine.PluginConfig, schema string, storageUnit string) (bool, error) {
	return false, errors.ErrUnsupported
}

func (p *SpannerPlugin) DeleteRow(config *engine.PluginConfig, schema string, storageUnit string, values map[string]string) (bool, error) {
	return false, errors.ErrUnsupported
}
