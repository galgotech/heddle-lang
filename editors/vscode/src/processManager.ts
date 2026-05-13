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
            const isWindows = process.platform === 'win32';
            const child = cp.spawn(command, args, {
                cwd,
                shell: false, // Disable shell to get the direct PID of the process
                detached: !isWindows // Create a new process group on Unix
            });

            if (child.pid) {
                this.outputChannel.appendLine(`[${prefix}] Process started with PID: ${child.pid}`);
            }

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
            const pid = child.pid;
            this.outputChannel.appendLine(`Stopping process: ${id} (PID: ${pid})`);
            
            if (pid) {
                try {
                    if (process.platform !== 'win32') {
                        // 1. Try to kill the entire process group with SIGTERM
                        try {
                            process.kill(-pid, 'SIGTERM');
                        } catch (e) {
                            // If group kill fails (e.g. not a leader), kill the process itself
                            try { process.kill(pid, 'SIGTERM'); } catch (e2) {}
                        }
                        
                        // 2. Set a more aggressive timeout for SIGKILL
                        setTimeout(() => {
                            try {
                                // Check if process still exists
                                process.kill(pid, 0);
                                // If it does, be more aggressive
                                this.outputChannel.appendLine(`Process ${id} (${pid}) still running after SIGTERM, sending SIGKILL...`);
                                try { process.kill(-pid, 'SIGKILL'); } catch (e) {
                                    try { process.kill(pid, 'SIGKILL'); } catch (e2) {}
                                }
                            } catch (e) {
                                // Process is already gone
                            }
                        }, 1000);
                    } else {
                        // On Windows, use taskkill to kill the process tree
                        cp.exec(`taskkill /pid ${pid} /T /F`);
                    }
                } catch (err: any) {
                    this.outputChannel.appendLine(`Error killing process ${id}: ${err.message}`);
                }
            } else {
                child.kill('SIGKILL');
            }

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
