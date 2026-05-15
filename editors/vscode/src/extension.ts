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

let client: LanguageClient;
let configManager: ConfigurationManager;

export async function activate(context: ExtensionContext) {
    const outputChannel = window.createOutputChannel("Heddle");
    context.subscriptions.push(outputChannel);

    configManager = new ConfigurationManager(workspace, () => {
        startServices();
    });

    const terminalManager = new TerminalManager();
    context.subscriptions.push({ dispose: () => terminalManager.dispose() });

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
            args: ['dev', 'lsp', '--control-plane-addr', cpAddr],
            options: {
                cwd: context.extensionPath
            },
        };

        console.log(serverOptions);

        const clientOptions: LanguageClientOptions = {
            documentSelector: [
                { scheme: 'file', language: 'heddle' },
                { scheme: 'untitled', language: 'heddle' }
            ],
            outputChannel: outputChannel,
            synchronize: {
                fileEvents: workspace.createFileSystemWatcher('**/*.he')
            }
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

        let heddlePath = workspace.getConfiguration('heddle').get<string>('clientPath') || getHeddlePath();
        const cmd = `${heddlePath} run "${fileUri.fsPath}"`;
        outputChannel.appendLine(`Running command: ${cmd}`);
        terminalManager.executeCommand(cmd);
    }));

    context.subscriptions.push(commands.registerCommand('heddle.runWorkflow', async (args: { uri: string, workflow: string }) => {
        const fileUri = Uri.parse(args.uri);
        let heddlePath = workspace.getConfiguration('heddle').get<string>('clientPath') || getHeddlePath();
        const cmd = `${heddlePath} run "${fileUri.fsPath}" --workflow "${args.workflow}"`;
        outputChannel.appendLine(`Running workflow command: ${cmd}`);
        terminalManager.executeCommand(cmd);
    }));

    context.subscriptions.push(commands.registerCommand('heddle.debugWorkflow', async (args: { uri: string, workflow: string }) => {
        const fileUri = Uri.parse(args.uri);
        debug.startDebugging(workspace.getWorkspaceFolder(fileUri), {
            type: 'heddle-debug',
            name: `Debug ${args.workflow}`,
            request: 'launch',
            program: fileUri.fsPath,
            workflow: args.workflow
        });
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
