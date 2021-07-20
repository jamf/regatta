package replication

import (
	"context"
	"errors"
	"time"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/storage/tables"
	"go.uber.org/zap"
)

// NewMetadata returns a new instance of Metadata replicator.
func NewMetadata(client proto.MetadataClient, manager *tables.Manager) *Metadata {
	return &Metadata{
		MetadataClient: client,
		TableManager:   manager,
		Interval:       30 * time.Second,
		closer:         make(chan struct{}),
		log:            zap.S().Named("replication").Named("metadata"),
	}
}

// Metadata connects to the metadata service and synchronise the state locally.
type Metadata struct {
	MetadataClient proto.MetadataClient
	TableManager   *tables.Manager
	Interval       time.Duration
	closer         chan struct{}
	log            *zap.SugaredLogger
}

// Replicate launches the replication goroutine. To stop it call Close.
func (m *Metadata) Replicate() {
	go func() {
		m.log.Info("replication started")
		t := time.NewTicker(m.Interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				metadataResponse, err := m.MetadataClient.Get(context.TODO(), &proto.MetadataRequest{})
				if err != nil {
					m.log.Errorf("metadata request failed: %v", err)
					continue
				}
				for _, tabs := range metadataResponse.GetTables() {
					if err := m.TableManager.CreateTable(tabs.Name); err != nil && !errors.Is(err, tables.ErrTableExists) {
						m.log.Errorf("cannot create table %s: %v", tabs.Name, err)
					}
				}
			case <-m.closer:
				m.log.Info("replication stopped")
				return
			}
		}
	}()
}

// Close stops the replication.
func (m *Metadata) Close() {
	close(m.closer)
}
