import * as vscode from 'vscode';
import { HeddleCompletionService } from './completionService';
import { HeddleHoverService } from './hoverService';

export class HeddleCompletionItemProvider implements vscode.CompletionItemProvider {
    constructor(private service: HeddleCompletionService) {}

    provideCompletionItems(document: vscode.TextDocument, position: vscode.Position, token: vscode.CancellationToken, context: vscode.CompletionContext): vscode.ProviderResult<vscode.CompletionItem[] | vscode.CompletionList> {
        const offset = document.offsetAt(position);
        const items = this.service.getCompletions(document.getText(), offset);
        
        return items.map(i => {
            // Map kind 14 to Keyword
            const item = new vscode.CompletionItem(i.label, vscode.CompletionItemKind.Keyword);
            return item;
        });
    }
}

export class HeddleHoverProvider implements vscode.HoverProvider {
    constructor(private service: HeddleHoverService) {}

    async provideHover(document: vscode.TextDocument, position: vscode.Position, token: vscode.CancellationToken): Promise<vscode.Hover | null> {
        const range = document.getWordRangeAtPosition(position);
        if (!range) {
            return null;
        }
        const word = document.getText(range);
        
        const data = await this.service.getHover(word);
        if (!data) {
            return null;
        }

        return new vscode.Hover(new vscode.MarkdownString(data.markdown));
    }
}
