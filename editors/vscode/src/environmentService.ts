import * as path from 'path';
import * as fs from 'fs';
import * as cp from 'child_process';

export class EnvironmentService {
    async getPoetryInterpreterPath(workspaceFolder: string): Promise<string | undefined> {
        const heddlePrivatePath = path.resolve(workspaceFolder, '../heddle-private');

        if (!fs.existsSync(heddlePrivatePath)) {
            return undefined;
        }

        // 1. Check for .venv directory
        const venvPath = path.join(heddlePrivatePath, '.venv');
        if (fs.existsSync(venvPath)) {
            const pythonPath = path.join(venvPath, 'bin', 'python');
            if (fs.existsSync(pythonPath)) {
                return pythonPath;
            }
        }

        // 2. Fallback to `poetry env info --path`
        try {
            const envPath = await this.runPoetryEnvInfo(heddlePrivatePath);
            if (envPath) {
                return path.join(envPath, 'bin', 'python');
            }
        } catch (error) {
            // Ignore error
        }

        return undefined;
    }

    private runPoetryEnvInfo(cwd: string): Promise<string> {
        return new Promise((resolve, reject) => {
            cp.exec('poetry env info --path', { cwd }, (error, stdout, stderr) => {
                if (error) {
                    reject(error);
                } else {
                    resolve(stdout.trim());
                }
            });
        });
    }
}