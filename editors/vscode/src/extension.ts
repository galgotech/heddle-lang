import * as path from 'path';
import * as fs from 'fs';
import { workspace, ExtensionContext, debug, DebugAdapterDescriptorFactory, DebugSession, DebugAdapterDescriptor, DebugAdapterExecutable, window, commands, Uri, OutputChannel } from 'vscode';
import {
    LanguageClient,
    LanguageClientOptions,
    ServerOptions,
} from 'vscode-languageclient/node';
import { ConfigurationManager } from './configuration';
import { EnvironmentService } from './environmentService';
import { TerminalManager } from './terminalManager';

let client: LanguageClient;
let configManager: ConfigurationManager;

export async function activate(context: ExtensionContext) {
    const outputChannel = window.createOutputChannel("Heddle");
    context.subscriptions.push(outputChannel);
    outputChannel.appendLine("Heddle Extension Activating...");

    let providersDisposables: Disposable[] = [];
    const diagnosticCollection = languages.createDiagnosticCollection('heddle');
    context.subscriptions.push(diagnosticCollection);

    const envService = new EnvironmentService();
    const terminalManager = new TerminalManager();
    context.subscriptions.push({ dispose: () => terminalManager.dispose() });

    const startServices = async () => {
        let lspPath = workspace.getConfiguration('heddle').get<string>('lspPath');
        if (!lspPath) {
            lspPath = path.join(context.extensionPath, '..', '..', 'bin', 'heddle-lsp');
        }
        outputChannel.appendLine(`Starting Heddle LSP from: '${lspPath}'`);

        if (client) {
            await client.stop();
        }

        const serverOptions: ServerOptions = {
            command: lspPath,
            args: [],
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

        let clientPath = workspace.getConfiguration('heddle').get<string>('clientPath');
        if (!clientPath) {
            clientPath = path.join(context.extensionPath, '..', '..', 'bin', 'heddle-client');
        }

        const cmd = `${clientPath} submit "${fileUri.fsPath}"`;
        outputChannel.appendLine(`Running command: ${cmd}`);
        terminalManager.executeCommand(cmd);
    }));
}

export function deactivate(): Thenable<void> | undefined {
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
        let dapPath = workspace.getConfiguration('heddle').get<string>('dapPath');
        if (!dapPath) {
            dapPath = path.join(this.context.extensionPath, '..', '..', 'bin', 'heddle-dap');
        }

        const workspaceFolder = workspace.workspaceFolders?.[0]?.uri.fsPath || '';
        const cwd = workspaceFolder || this.context.extensionPath;
        this.outputChannel.appendLine(`Launching Debug Adapter: command='${dapPath}', cwd='${cwd}'`);

        return new DebugAdapterExecutable(dapPath, [], {
            cwd: cwd,
            env: process.env as { [key: string]: string },
        });
    }
}
