package development

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	debugadapter "github.com/galgotech/heddle-lang/internal/services/debug-adapter"
	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var dapCfgFile string

// DapCmd starts the Heddle Debug Adapter
var DapCmd = &cobra.Command{
	Use:   "dap",
	Short: "Start the Heddle Debug Adapter",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Init("HEDDLE_DAP", dapCfgFile)
	},
	Run: func(cmd *cobra.Command, args []string) {
		err := logger.Init(logger.Config{
			Development: true,
			OutputPaths: []string{"stdout", "heddle-dap.log"},
		})
		if err != nil {
			panic(err)
		}
		defer logger.Sync()

		logger.L().Info("Heddle Debug Adapter starting")

		if viper.GetBool("server") {
			debugadapter.StartServer(viper.GetString("addr"))
		} else {
			debugadapter.Serve(os.Stdin, os.Stdout)
		}
	},
}

func init() {
	DapCmd.PersistentFlags().StringVar(&dapCfgFile, "config", "", "config file (default is ./heddle-dap.yaml)")
	DapCmd.Flags().Bool("server", false, "Start in server mode")
	DapCmd.Flags().String("addr", "localhost:4711", "Address to listen on in server mode")

	viper.BindPFlag("server", DapCmd.Flags().Lookup("server"))
	viper.BindPFlag("addr", DapCmd.Flags().Lookup("addr"))
}
