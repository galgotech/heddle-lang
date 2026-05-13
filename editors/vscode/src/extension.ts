import * as path from 'path';
import {
    workspace,
    ExtensionContext,
    debug,
    DebugAdapterDescriptorFactory,
    DebugSession,
    DebugAdapterDescriptor,
    DebugAdapterExecutable,
    window,
    commands,
    Uri,
    OutputChannel,
} from 'vscode';
import {
    LanguageClient,
    LanguageClientOptions,
    ServerOptions,
} from 'vscode-languageclient/node';
import { ConfigurationManager } from './configuration';
import { TerminalManager } from './terminalManager';
import { ProcessManager } from './processManager';
import { ControlPlaneTreeDataProvider, SdkPluginsTreeDataProvider, HeddleTreeItem } from './treeViews';

let client: LanguageClient;
let configManager: ConfigurationManager;
let processManager: ProcessManager;

export async function activate(context: ExtensionContext) {
    const outputChannel = window.createOutputChannel("Heddle");
    const processOutputChannel = window.createOutputChannel("Heddle Logs");
    context.subscriptions.push(outputChannel);
    context.subscriptions.push(processOutputChannel);

    processManager = new ProcessManager(processOutputChannel);
    context.subscriptions.push({ dispose: () => processManager.stopAll() });

    configManager = new ConfigurationManager(workspace, () => {
        startServices();
    });

    const terminalManager = new TerminalManager();
    context.subscriptions.push({ dispose: () => terminalManager.dispose() });

    // Tree Views
    const controlPlaneProvider = new ControlPlaneTreeDataProvider(processManager);
    window.registerTreeDataProvider('heddle-control-plane', controlPlaneProvider);

    const sdkPluginsProvider = new SdkPluginsTreeDataProvider(processManager, context);
    window.registerTreeDataProvider('heddle-sdk-plugins', sdkPluginsProvider);

    // Status Bar Item for AOT Validation
    const aotStatus = window.createStatusBarItem(2, 100); // StatusBarAlignment.Right
    aotStatus.text = "$(shield) Heddle AOT: Active";
    aotStatus.tooltip = "Heddle Ahead-Of-Time Type Checking is active";
    aotStatus.show();
    context.subscriptions.push(aotStatus);

    const getHeddlePath = () => {
        let heddlePath = workspace.getConfiguration('heddle').get<string>('path');
        if (!heddlePath) {
            heddlePath = path.join(context.extensionPath, '..', '..', 'bin', 'heddle');
        }
        return heddlePath;
    };

    const startServices = async () => {
        let heddlePath = workspace.getConfiguration('heddle').get<string>('lspPath') || getHeddlePath();
        const cpAddr = configManager.getControlPlaneAddr();
        outputChannel.appendLine(`Starting Heddle LSP using: '${heddlePath}' at ${cpAddr}`);

        if (client) {
            await client.stop();
        }

        const serverOptions: ServerOptions = {
            command: heddlePath,
            args: ['development', 'lsp', '--control-plane-addr', cpAddr],
            options: {
                cwd: context.extensionPath
            },
        };

        const clientOptions: LanguageClientOptions = {
            documentSelector: [{ scheme: 'file', language: 'heddle' }],
            outputChannel: outputChannel
        };

        client = new LanguageClient(
            'heddleLanguageServer',
            'Heddle Language Server',
            serverOptions,
            clientOptions
        );

        try {
            await client.start();
            outputChannel.appendLine("Language Server started successfully.");
        } catch (e) {
            window.showErrorMessage(`Heddle Language Server failed to start: ${e}`);
            outputChannel.appendLine(`Error starting LS: ${e}`);
        }
    };

    await startServices();

    // Register Commands
    context.subscriptions.push(commands.registerCommand('heddle.startControlPlane', async () => {
        const heddlePath = getHeddlePath();
        const cwd = workspace.workspaceFolders?.[0]?.uri.fsPath || context.extensionPath;
        await processManager.start('control-plane', heddlePath, ['local', 'start'], cwd, 'Control Plane');
    }));

    context.subscriptions.push(commands.registerCommand('heddle.stopControlPlane', () => {
        processManager.stop('control-plane');
    }));

    context.subscriptions.push(commands.registerCommand('heddle.restartControlPlane', async () => {
        processManager.stop('control-plane');
        await commands.executeCommand('heddle.startControlPlane');
    }));

    context.subscriptions.push(commands.registerCommand('heddle.addPluginFolder', async () => {
        const folders = await window.showOpenDialog({
            canSelectFiles: false,
            canSelectFolders: true,
            canSelectMany: false,
            openLabel: 'Select Plugin Folder'
        });

        if (folders && folders.length > 0) {
            sdkPluginsProvider.addFolder(folders[0].fsPath);
        }
    }));

    context.subscriptions.push(commands.registerCommand('heddle.startPlugin', async (item: HeddleTreeItem) => {
        if (item && item.path) {
            const heddlePath = getHeddlePath();
            await processManager.start(item.itemId, heddlePath, ['local', 'start'], item.path, `Plugin: ${path.basename(item.path)}`);
        }
    }));

    context.subscriptions.push(commands.registerCommand('heddle.stopPlugin', (item: HeddleTreeItem) => {
        if (item) {
            processManager.stop(item.itemId);
        }
    }));

    context.subscriptions.push(commands.registerCommand('heddle.restartPlugin', async (item: HeddleTreeItem) => {
        if (item) {
            processManager.stop(item.itemId);
            await commands.executeCommand('heddle.startPlugin', item);
        }
    }));

    context.subscriptions.push(commands.registerCommand('heddle.editPluginSource', (uri: Uri) => {
        if (uri) {
            commands.executeCommand('vscode.openFolder', uri, true);
        }
    }));

    // Register Debug Adapter
    const factory = new HeddleDebugAdapterDescriptorFactory(context, configManager, outputChannel);
    context.subscriptions.push(debug.registerDebugAdapterDescriptorFactory('heddle-debug', factory));

    // Register Run Command
    context.subscriptions.push(commands.registerCommand('heddle.runFile', async (uri?: Uri) => {
        const fileUri = uri || window.activeTextEditor?.document.uri;
        if (!fileUri) {
            window.showErrorMessage("No file selected to run.");
            return;
        }

        let heddlePath = workspace.getConfiguration('heddle').get<string>('clientPath') || getHeddlePath();
        const cmd = `${heddlePath} run "${fileUri.fsPath}"`;
        outputChannel.appendLine(`Running command: ${cmd}`);
        terminalManager.executeCommand(cmd);
    }));
}

export function deactivate(): Thenable<void> | undefined {
    if (processManager) {
        processManager.stopAll();
    }
    if (!client) {
        return undefined;
    }
    return client.stop();
}

class HeddleDebugAdapterDescriptorFactory implements DebugAdapterDescriptorFactory {
    private context: ExtensionContext;
    private configManager: ConfigurationManager;
    private outputChannel: OutputChannel;

    constructor(context: ExtensionContext, configManager: ConfigurationManager, outputChannel: OutputChannel) {
        this.context = context;
        this.configManager = configManager;
        this.outputChannel = outputChannel;
    }

    async createDebugAdapterDescriptor(session: DebugSession, executable: DebugAdapterExecutable | undefined): Promise<DebugAdapterDescriptor> {
        let heddlePath = workspace.getConfiguration('heddle').get<string>('dapPath') || workspace.getConfiguration('heddle').get<string>('path');
        if (!heddlePath) {
            heddlePath = path.join(this.context.extensionPath, '..', '..', 'bin', 'heddle');
        }

        const workspaceFolder = workspace.workspaceFolders?.[0]?.uri.fsPath || '';
        const cwd = workspaceFolder || this.context.extensionPath;
        this.outputChannel.appendLine(`Launching Debug Adapter: command='${heddlePath}', args='development dap', cwd='${cwd}'`);

        return new DebugAdapterExecutable(heddlePath, ['development', 'dap'], {
            cwd: cwd,
            env: process.env as { [key: string]: string },
        });
    }
}

