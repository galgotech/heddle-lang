package terminal

import (
	"fmt"
	"strings"

	"github.com/galgotech/heddle-lang/pkg/dx/analyzer"
)

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorYellow = "\033[33m"
	ColorCyan   = "\033[36m"
	ColorBold   = "\033[1m"
	ColorGray   = "\033[90m"
)

// Reporter handles visual diagnostic output to the terminal.
type Reporter struct {
	source string
	lines  []string
}

// NewReporter initializes a reporter with the source code.
func NewReporter(source string) *Reporter {
	return &Reporter{
		source: source,
		lines:  strings.Split(source, "\n"),
	}
}

// Report prints a list of diagnostics in a rich, Rust-style format.
func (r *Reporter) Report(diagnostics []analyzer.Diagnostic) {
	for _, d := range diagnostics {
		r.reportOne(d)
	}
}

func (r *Reporter) reportOne(d analyzer.Diagnostic) {
	severityLabel := "error"
	color := ColorRed
	if d.Severity == analyzer.Warning {
		severityLabel = "warning"
		color = ColorYellow
	}

	fmt.Printf("%s%s[%s]: %s%s%s\n", ColorBold, color, severityLabel, ColorReset, ColorBold, d.Message)

	lineIdx := int(d.Range.Start.Line) - 1
	if lineIdx >= 0 && lineIdx < len(r.lines) {
		line := r.lines[lineIdx]

		// Print line number and code snippet
		fmt.Printf("%s %3d | %s%s\n", ColorGray, d.Range.Start.Line, ColorReset, line)

		// Print underline (^^^^)
		padding := strings.Repeat(" ", int(d.Range.Start.Col)+5) // 5 for " 3d | "
		underlineLen := int(d.Range.End.Col - d.Range.Start.Col)
		if underlineLen <= 0 {
			underlineLen = 1
		}
		underline := strings.Repeat("^", underlineLen)
		fmt.Printf("%s%s%s%s\n", padding, color, underline, ColorReset)
	}

	if d.Help != "" {
		fmt.Printf("%s%shelp: %s%s%s\n", ColorGray, ColorBold, ColorReset, ColorGray, d.Help)
	}
	fmt.Println()
}
