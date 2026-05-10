package plugin_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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

	err := p.Start()
	require.NoError(t, err)
}
