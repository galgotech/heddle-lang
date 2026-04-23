import * as vscode from 'vscode';

export class TerminalManager {
    private terminal: vscode.Terminal | undefined;

    public getTerminal(): vscode.Terminal {
        if (!this.terminal || this.terminal.exitStatus !== undefined) {
             this.terminal = vscode.window.createTerminal("Heddle Output");
        }
        return this.terminal;
    }
    
    public executeCommand(command: string) {
        const terminal = this.getTerminal();
        terminal.show(true); // Preserve focus
        terminal.sendText(command);
    }
    
    public dispose() {
        if (this.terminal) {
            this.terminal.dispose();
            this.terminal = undefined;
        }
    }
}
