import * as cp from 'child_process';
import * as vscode from 'vscode';

export enum ProcessStatus {
    Stopped,
    Running
}

export class ProcessManager {
    private processes: Map<string, cp.ChildProcess> = new Map();
    private outputChannel: vscode.OutputChannel;
    private _onStatusChange = new vscode.EventEmitter<string>();
    public readonly onStatusChange = this._onStatusChange.event;

    constructor(outputChannel: vscode.OutputChannel) {
        this.outputChannel = outputChannel;
    }

    public async start(id: string, command: string, args: string[], cwd: string, prefix: string): Promise<void> {
        if (this.processes.has(id)) {
            return;
        }

        this.outputChannel.appendLine(`[${prefix}] Starting process: ${command} ${args.join(' ')} in ${cwd}`);

        try {
            const child = cp.spawn(command, args, {
                cwd,
                shell: true // Useful for some platforms/commands
            });

            child.stdout?.on('data', (data) => {
                this.formatLog(prefix, data);
            });

            child.stderr?.on('data', (data) => {
                this.formatLog(prefix, data, true);
            });

            child.on('close', (code) => {
                this.outputChannel.appendLine(`[${prefix}] Process exited with code ${code}`);
                this.processes.delete(id);
                this.updateContext(id, ProcessStatus.Stopped);
                this._onStatusChange.fire(id);
            });

            child.on('error', (err) => {
                this.outputChannel.appendLine(`[${prefix}] Failed to start process: ${err.message}`);
                this.processes.delete(id);
                this.updateContext(id, ProcessStatus.Stopped);
                this._onStatusChange.fire(id);
            });

            this.processes.set(id, child);
            this.updateContext(id, ProcessStatus.Running);
            this._onStatusChange.fire(id);
        } catch (err: any) {
            this.outputChannel.appendLine(`[${prefix}] Error spawning process: ${err.message}`);
            vscode.window.showErrorMessage(`Failed to start Heddle process (${prefix}): ${err.message}`);
        }
    }

    private formatLog(prefix: string, data: any, isError = false) {
        const lines = data.toString().split(/\r?\n/);
        for (const line of lines) {
            if (line.trim()) {
                this.outputChannel.appendLine(`[${prefix}]${isError ? ' [ERR]' : ''} ${line}`);
            }
        }
    }

    public stop(id: string): void {
        const child = this.processes.get(id);
        if (child) {
            this.outputChannel.appendLine(`Stopping process: ${id}`);
            // Use a more forceful kill if necessary, but start with SIGTERM
            child.kill();
            this.processes.delete(id);
            this.updateContext(id, ProcessStatus.Stopped);
            this._onStatusChange.fire(id);
        }
    }

    public stopAll(): void {
        for (const id of Array.from(this.processes.keys())) {
            this.stop(id);
        }
    }

    public isRunning(id: string): boolean {
        return this.processes.has(id);
    }

    private updateContext(id: string, status: ProcessStatus) {
        if (id === 'control-plane') {
            vscode.commands.executeCommand('setContext', 'heddle.controlPlaneRunning', status === ProcessStatus.Running);
            vscode.commands.executeCommand('setContext', 'heddle.controlPlaneStopped', status === ProcessStatus.Stopped);
        }
    }
}
