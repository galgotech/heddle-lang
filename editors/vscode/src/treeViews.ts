import * as vscode from 'vscode';
import * as path from 'path';
import { ProcessManager } from './processManager';

export class HeddleTreeItem extends vscode.TreeItem {
    constructor(
        public readonly label: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly contextValue: string,
        public readonly itemId: string,
        public readonly path?: string
    ) {
        super(label, collapsibleState);
    }
}

export class ControlPlaneTreeDataProvider implements vscode.TreeDataProvider<HeddleTreeItem> {
    private _onDidChangeTreeData: vscode.EventEmitter<HeddleTreeItem | undefined | null | void> = new vscode.EventEmitter<HeddleTreeItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<HeddleTreeItem | undefined | null | void> = this._onDidChangeTreeData.event;

    constructor(private processManager: ProcessManager) {
        this.processManager.onStatusChange((id) => {
            if (id === 'control-plane') {
                this.refresh();
            }
        });
    }

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: HeddleTreeItem): vscode.TreeItem {
        return element;
    }

    getChildren(element?: HeddleTreeItem): Thenable<HeddleTreeItem[]> {
        if (element) {
            return Promise.resolve([]);
        }

        const isRunning = this.processManager.isRunning('control-plane');
        const contextValue = isRunning ? 'controlPlaneRunning' : 'controlPlaneStopped';
        const label = `Local Control Plane`;
        const item = new HeddleTreeItem(label, vscode.TreeItemCollapsibleState.None, contextValue, 'control-plane');
        item.description = isRunning ? 'Running' : 'Stopped';
        item.iconPath = new vscode.ThemeIcon(isRunning ? 'pass' : 'circle-outline', isRunning ? new vscode.ThemeColor('debugIcon.startForeground') : undefined);

        return Promise.resolve([item]);
    }
}

export class SdkPluginsTreeDataProvider implements vscode.TreeDataProvider<HeddleTreeItem> {
    private _onDidChangeTreeData: vscode.EventEmitter<HeddleTreeItem | undefined | null | void> = new vscode.EventEmitter<HeddleTreeItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<HeddleTreeItem | undefined | null | void> = this._onDidChangeTreeData.event;

    private plugins: string[] = [];

    constructor(private processManager: ProcessManager, private context: vscode.ExtensionContext) {
        this.plugins = this.context.globalState.get<string[]>('heddle.pluginFolders', []);
        this.processManager.onStatusChange((id) => {
            if (id.startsWith('plugin:')) {
                this.refresh();
            }
        });
    }

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    addFolder(folder: string) {
        if (!this.plugins.includes(folder)) {
            this.plugins.push(folder);
            this.context.globalState.update('heddle.pluginFolders', this.plugins);
            this.refresh();
        } else {
            vscode.window.showInformationMessage('Folder is already added as a plugin.');
        }
    }

    removeFolder(folder: string) {
        this.plugins = this.plugins.filter(p => p !== folder);
        this.context.globalState.update('heddle.pluginFolders', this.plugins);
        this.refresh();
    }

    getTreeItem(element: HeddleTreeItem): vscode.TreeItem {
        return element;
    }

    getChildren(element?: HeddleTreeItem): Thenable<HeddleTreeItem[]> {
        if (element) {
            return Promise.resolve([]);
        }

        return Promise.resolve(this.plugins.map(p => {
            const itemId = `plugin:${p}`;
            const isRunning = this.processManager.isRunning(itemId);
            const contextValue = isRunning ? 'pluginRunning' : 'pluginStopped';
            const name = path.basename(p);
            const item = new HeddleTreeItem(name, vscode.TreeItemCollapsibleState.None, contextValue, itemId, p);
            item.description = isRunning ? 'Running' : 'Stopped';
            item.iconPath = new vscode.ThemeIcon('symbol-package');
            item.tooltip = p;
            return item;
        }));
    }
}
