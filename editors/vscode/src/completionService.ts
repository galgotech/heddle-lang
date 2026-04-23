export interface CompletionData {
    label: string;
    kind: number; 
}

export class HeddleCompletionService {
    private static readonly KEYWORDS = ['import', 'step', 'pipeline', 'type', 'run'];

    getCompletions(documentText: string, offset: number): CompletionData[] {
        // Basic implementation: always return keywords
        return HeddleCompletionService.KEYWORDS.map(k => ({
            label: k,
            kind: 14 // Keyword
        }));
    }
}