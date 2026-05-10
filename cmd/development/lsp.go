package development

import (
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/galgotech/heddle-lang/pkg/config"
)

var lspCfgFile string

type stdioRW struct {
	io.Reader
	io.Writer
}

func (stdioRW) Close() error {
	return nil
}

// LspCmd starts the Heddle Language Server
var LspCmd = &cobra.Command{
	Use:   "lsp",
	Short: "Start the Heddle Language Server",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Init("HEDDLE_LSP", lspCfgFile)
	},
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func init() {
	LspCmd.PersistentFlags().StringVar(&lspCfgFile, "config", "", "config file (default is ./heddle-lsp.yaml)")
	LspCmd.Flags().String("log-path", "/tmp/heddle-lsp.log", "Path to log file")
	viper.BindPFlag("log-path", LspCmd.Flags().Lookup("log-path"))
}
