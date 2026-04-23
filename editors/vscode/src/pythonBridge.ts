import * as cp from 'child_process';
import * as fs from 'fs';

export class PythonBridge {
    private command: string;
    private baseArgs: string[];

    constructor(pythonPath: string) {
        // If path contains spaces and does NOT exist as a file, treat as command + args
        if (pythonPath.indexOf(' ') > 0 && !fs.existsSync(pythonPath)) {
             const parts = pythonPath.split(' ');
             this.command = parts[0];
             this.baseArgs = parts.slice(1);
        } else {
            this.command = pythonPath;
            this.baseArgs = [];
        }
    }

    async getHeddleVersion(): Promise<string> {
        return this.run(['-m', 'heddle', '--version']);
    }

    async executeCommand(args: string[]): Promise<string> {
        return this.run(['-m', 'heddle', ...args]);
    }

    private run(args: string[]): Promise<string> {
        return new Promise((resolve, reject) => {
            const allArgs = [...this.baseArgs, ...args];
            cp.execFile(this.command, allArgs, (error, stdout, stderr) => {
                if (error) {
                    const e: any = new Error(`Command failed: ${error.message}`);
                    e.stdout = stdout;
                    e.stderr = stderr;
                    e.code = error.code;
                    reject(e);
                } else {
                    resolve(stdout.trim());
                }
            });
        });
    }
}