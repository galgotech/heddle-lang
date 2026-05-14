package dev

import (
	"github.com/spf13/cobra"
)

// DevCmd is the root command for development and debugging tools.
var DevCmd = &cobra.Command{
	Use:   "dev",
	Short: "Development and debugging tools",
	Long:  `Development tools include the Heddle Language Server (LSP) and the Debug Adapter (DAP).`,
}

func init() {
	DevCmd.AddCommand(LspCmd)
	DevCmd.AddCommand(DapCmd)
	DevCmd.AddCommand(completionCmd)
}

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate completion script",
	Long: `To load completions:

Bash:

  $ source <(heddle dev completion bash)

  # To load completions for each session, add to your ~/.bashrc:
  $ heddle dev completion bash > /etc/bash_completion.d/heddle

Zsh:

  # If shell completion is not already enabled in your environment,
  # you will need to enable it.  You can execute the following once:

  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

  # To load completions for each session, execute once:
  $ heddle dev completion zsh > "${fpath[1]}/_heddle"

  # You will need to start a new shell for this setup to take effect.

Fish:

  $ heddle dev completion fish | source

  # To load completions for each session, execute once:
  $ heddle dev completion fish > ~/.config/fish/completions/heddle.fish

PowerShell:

  PS> heddle dev completion powershell | Out-String | Invoke-Expression

  # To load completions for every new session, run:
  PS> heddle dev completion powershell > heddle.ps1
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
