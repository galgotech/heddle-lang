package plugin_test

import (
	"context"
	"testing"
	"time"

	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type VaultResource struct {
	ApiKey string `json:"api_key"`
}

func (r *VaultResource) Start(ctx context.Context) error {
	return nil
}

type HashConfig struct {
	plugin.Config
	Algorithm string `json:"algorithm"`
}

type CredentialsTable struct {
	core.Table
	id       int
	password string
}

type SecureTable struct {
	core.Table
	id   int
	hash string
}

func HashPassword(ctx context.Context, cfg HashConfig, input CredentialsTable) (SecureTable, error) {
	record := input.Native()
	_ = cfg.GetResource().(*VaultResource).ApiKey
	return SecureTable{
		Table: core.NewTableFromRecord(record),
	}, nil
}

func TestExampleInterface(t *testing.T) {
	p := plugin.New("security")

	p.RegisterResource("vault", &VaultResource{})
	p.RegisterStep("hash", HashPassword)

	// Start the plugin (it will monitor signals internally)
	// We run it in a goroutine because it blocks
	go func() {
		_ = p.Start()
	}()
	
	// Wait a bit to ensure it starts
	time.Sleep(100 * time.Millisecond)
}
