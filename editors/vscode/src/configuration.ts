import type { WorkspaceConfiguration, ConfigurationChangeEvent, Disposable } from 'vscode';
import { EnvironmentService } from './environmentService';

export interface IWorkspaceAdapter {
    getConfiguration(section: string): WorkspaceConfiguration;
    onDidChangeConfiguration(listener: (e: ConfigurationChangeEvent) => any): Disposable;
}

export class ConfigurationManager {
    constructor(
        private adapter: IWorkspaceAdapter, 
        private onPythonPathChange: (newPath: string) => void,
        private envService?: EnvironmentService
    ) {
        this.adapter.onDidChangeConfiguration((e) => {
            if (e.affectsConfiguration('heddle.pythonPath')) {
                // We just notify that something changed, the consumer should call getPythonPath
                // But wait, onPythonPathChange expects a string.
                // We should probably re-evaluate here?
                // For now let's keep existing logic but we might need to change what we pass
                const config = this.adapter.getConfiguration('heddle');
                const newPath = config.get<string>('pythonPath', 'python');
                if (newPath) {
                    this.onPythonPathChange(newPath);
                }
            }
        });
    }

    async getPythonPath(workspaceFolder: string): Promise<string> {
        if (this.envService) {
            const detected = await this.envService.getPoetryInterpreterPath(workspaceFolder);
            if (detected) {
                return detected;
            }
        }
        const config = this.adapter.getConfiguration('heddle');
        return config.get<string>('pythonPath', 'python') || 'python';
    }
}