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

    private remotes: string[] = [];

    constructor(private processManager: ProcessManager, private context: vscode.ExtensionContext) {
        this.remotes = this.context.globalState.get<string[]>('heddle.remoteControlPlanes', []);
        this.processManager.onStatusChange((id) => {
            if (id === 'control-plane') {
                this.refresh();
            }
        });

        vscode.workspace.onDidChangeConfiguration(e => {
            if (e.affectsConfiguration('heddle.controlPlaneAddr')) {
                this.refresh();
            }
        });
    }

    addRemote(address: string) {
        if (!this.remotes.includes(address)) {
            this.remotes.push(address);
            this.context.globalState.update('heddle.remoteControlPlanes', this.remotes);
            this.refresh();
        } else {
            vscode.window.showInformationMessage('Remote control plane is already added.');
        }
    }

    removeRemote(address: string) {
        this.remotes = this.remotes.filter(r => r !== address);
        this.context.globalState.update('heddle.remoteControlPlanes', this.remotes);
        this.refresh();
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

        const items: HeddleTreeItem[] = [];
        const currentAddr = vscode.workspace.getConfiguration('heddle').get<string>('controlPlaneAddr', 'localhost:50051');

        // Local Control Plane
        const isRunning = this.processManager.isRunning('control-plane');
        const isLocalActive = currentAddr === 'localhost:50051';
        const contextValue = isRunning ? 'controlPlaneRunning' : 'controlPlaneStopped';
        const localItem = new HeddleTreeItem(`Local Control Plane`, vscode.TreeItemCollapsibleState.None, contextValue, 'control-plane');
        localItem.description = (isRunning ? 'Running' : 'Stopped') + (isLocalActive ? ' (Active)' : '');
        localItem.iconPath = new vscode.ThemeIcon(isRunning ? 'pass' : 'circle-outline', isRunning ? new vscode.ThemeColor('debugIcon.startForeground') : undefined);
        items.push(localItem);

        // Remote Control Planes
        this.remotes.forEach(remote => {
            const isActive = currentAddr === remote;
            const contextValue = isActive ? 'remoteControlPlaneConnected' : 'remoteControlPlaneDisconnected';
            const item = new HeddleTreeItem(remote, vscode.TreeItemCollapsibleState.None, contextValue, `remote:${remote}`);
            item.description = 'Remote' + (isActive ? ' (Connected)' : '');
            item.iconPath = new vscode.ThemeIcon(isActive ? 'cloud' : 'cloud-offline', isActive ? new vscode.ThemeColor('debugIcon.startForeground') : undefined);
            item.tooltip = `Remote Control Plane at ${remote}`;
            items.push(item);
        });

        return Promise.resolve(items);
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
