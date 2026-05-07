package development

import (
	"github.com/spf13/cobra"
)

// DevelopmentCmd is the root command for development and debugging tools.
var DevelopmentCmd = &cobra.Command{
	Use:   "development",
	Short: "Development and debugging tools",
	Long:  `Development tools include the Heddle Language Server (LSP) and the Debug Adapter (DAP).`,
}

func init() {
	DevelopmentCmd.AddCommand(LspCmd)
	DevelopmentCmd.AddCommand(DapCmd)
	DevelopmentCmd.AddCommand(completionCmd)
}

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate completion script",
	Long: `To load completions:

Bash:

  $ source <(heddle development completion bash)

  # To load completions for each session, add to your ~/.bashrc:
  $ heddle development completion bash > /etc/bash_completion.d/heddle

Zsh:

  # If shell completion is not already enabled in your environment,
  # you will need to enable it.  You can execute the following once:

  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

  # To load completions for each session, execute once:
  $ heddle development completion zsh > "${fpath[1]}/_heddle"

  # You will need to start a new shell for this setup to take effect.

Fish:

  $ heddle development completion fish | source

  # To load completions for each session, execute once:
  $ heddle development completion fish > ~/.config/fish/completions/heddle.fish

PowerShell:

  PS> heddle development completion powershell | Out-String | Invoke-Expression

  # To load completions for every new session, run:
  PS> heddle development completion powershell > heddle.ps1
  # and source this file from your PowerShell profile.
`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			cmd.Root().GenBashCompletion(cmd.OutOrStdout())
		case "zsh":
			cmd.Root().GenZshCompletion(cmd.OutOrStdout())
		case "fish":
			cmd.Root().GenFishCompletion(cmd.OutOrStdout(), true)
		case "powershell":
			cmd.Root().GenPowerShellCompletionWithDesc(cmd.OutOrStdout())
		}
	},
}
