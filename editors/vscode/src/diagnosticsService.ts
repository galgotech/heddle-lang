import { PythonBridge } from './pythonBridge';

export interface DiagnosticError {
    line: number;
    column: number;
    message: string;
    severity: 'error' | 'warning';
}

export class HeddleDiagnosticsService {
    constructor(private bridge: PythonBridge) {}

    async validate(filePath: string): Promise<DiagnosticError[]> {
        try {
            // Assume 'check <file>' runs syntax check
            await this.bridge.executeCommand(['check', filePath]);
            return [];
        } catch (e: any) {
            // Check if it's a command failure with output
            if (e.stdout) {
                return this.parseErrors(e.stdout);
            }
            return [];
        }
    }

    private parseErrors(output: string): DiagnosticError[] {
        const errors: DiagnosticError[] = [];
        // Pattern: filename:line:col: severity: message
        // Example: file.he:10:5: Error: Syntax error
        const regex = /([^:]+):(\d+):(\d+):\s*(Error|Warning):\s*(.*)/g;
        
        let match;
        while ((match = regex.exec(output)) !== null) {
            errors.push({
                line: parseInt(match[2]),
                column: parseInt(match[3]),
                message: match[5].trim(),
                severity: match[4].toLowerCase() === 'error' ? 'error' : 'warning'
            });
        }
        return errors;
    }
}