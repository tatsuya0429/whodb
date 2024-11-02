package spanner

import (
	"context"
	"fmt"

	spannerdb "cloud.google.com/go/spanner"
	"github.com/clidey/whodb/core/src/common"
	"github.com/clidey/whodb/core/src/engine"
)

func DB(config *engine.PluginConfig) (*spannerdb.Client, error) {
	ctx := context.Background()
	projectId := common.GetRecordValueOrDefault(config.Credentials.Advanced, "ProjectId", "test-project")
	instanceId := common.GetRecordValueOrDefault(config.Credentials.Advanced, "InstanceId", "test-instance")
	databaseId := config.Credentials.Database
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
	client, err := spannerdb.NewClient(ctx, databaseName)
	if err != nil {
		return nil, err
	}
	return client, nil
}
