import type { WorkspaceConfiguration, ConfigurationChangeEvent, Disposable } from 'vscode';

export interface IWorkspaceAdapter {
    getConfiguration(section: string): WorkspaceConfiguration;
    onDidChangeConfiguration(listener: (e: ConfigurationChangeEvent) => any): Disposable;
}

export class ConfigurationManager {
    constructor(
        private adapter: IWorkspaceAdapter,
        private onRestartRequired: () => void
    ) {
        this.adapter.onDidChangeConfiguration((e) => {
            if (e.affectsConfiguration('heddle.path') ||
                e.affectsConfiguration('heddle.lspPath') ||
                e.affectsConfiguration('heddle.controlPlaneAddr')) {
                this.onRestartRequired();
            }
        });
    }

    getControlPlaneAddr(): string {
        const config = this.adapter.getConfiguration('heddle');
        return config.get<string>('controlPlaneAddr', '/tmp/heddle-cp.sock');
    }
}