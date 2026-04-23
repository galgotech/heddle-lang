import { expect } from 'chai';
import { HeddleCompletionService } from '../completionService';

describe('HeddleCompletionService', () => {
    it('should return default keywords', () => {
        const service = new HeddleCompletionService();
        // Mock args
        const completions = service.getCompletions('', 0);
        const labels = completions.map(c => c.label);
        expect(labels).to.include('import');
        expect(labels).to.include('step');
        expect(labels).to.include('pipeline');
    });
});
