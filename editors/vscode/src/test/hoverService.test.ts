import { expect } from 'chai';
import * as sinon from 'sinon';
import { HeddleHoverService } from '../hoverService';
import { PythonBridge } from '../pythonBridge';

describe('HeddleHoverService', () => {
    let bridge: sinon.SinonStubbedInstance<PythonBridge>;
    let service: HeddleHoverService;

    beforeEach(() => {
        bridge = sinon.createStubInstance(PythonBridge);
        service = new HeddleHoverService(bridge as any);
    });

    it('should return hover for known step', async () => {
        // Mock python bridge response
        bridge.executeCommand.resolves(JSON.stringify({
            description: "A step that does something.",
            inputs: [],
            outputs: []
        }));

        const hover = await service.getHover('some.step');
        expect(hover).to.not.be.null;
        expect(hover?.markdown).to.include('A step that does something.');
        expect(bridge.executeCommand.calledWith(['inspect', 'some.step'])).to.be.true;
    });

    it('should return null for unknown step', async () => {
        bridge.executeCommand.rejects(new Error('Unknown step'));
        
        const hover = await service.getHover('unknown.step');
        expect(hover).to.be.null;
    });
});
