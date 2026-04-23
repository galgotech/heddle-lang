import { PythonBridge } from './pythonBridge';

export interface HoverData {
    markdown: string;
}

export class HeddleHoverService {
    constructor(private bridge: PythonBridge) {}

    async getHover(word: string): Promise<HoverData | null> {
        try {
            // Assume 'inspect <step_name>' returns JSON metadata
            const jsonOutput = await this.bridge.executeCommand(['inspect', word]);
            const data = JSON.parse(jsonOutput);
            
            return {
                markdown: `**${word}**\n\n${data.description}`
            };
        } catch (e) {
            return null;
        }
    }
}