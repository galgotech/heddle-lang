import { expect } from 'chai';
import * as sinon from 'sinon';
import { HeddleDiagnosticsService } from '../diagnosticsService';
import { PythonBridge } from '../pythonBridge';

describe('HeddleDiagnosticsService', () => {
    let bridge: sinon.SinonStubbedInstance<PythonBridge>;
    let service: HeddleDiagnosticsService;

    beforeEach(() => {
        bridge = sinon.createStubInstance(PythonBridge);
        service = new HeddleDiagnosticsService(bridge as any);
    });

    it('should parse errors correctly', async () => {
        const error: any = new Error('Command failed');
        error.stdout = 'file.he:10:5: Error: Syntax error\n';
        bridge.executeCommand.rejects(error);

        const errors = await service.validate('file.he');
        expect(errors).to.have.lengthOf(1);
        expect(errors[0].line).to.equal(10);
        expect(errors[0].message).to.equal('Syntax error');
    });

    it('should return empty array on success', async () => {
        bridge.executeCommand.resolves('');
        const errors = await service.validate('file.he');
        expect(errors).to.be.empty;
    });
});
