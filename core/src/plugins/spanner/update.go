package spanner

import (
	"errors"

	"github.com/clidey/whodb/core/src/engine"
)

func (p *SpannerPlugin) UpdateStorageUnit(config *engine.PluginConfig, schema string, storageUnit string, fields map[string]string) (bool, error) {
	db, err := DB(config)
	if err != nil {
		return false, err
	}
	defer db.Close()

	return false, errors.ErrUnsupported
}
