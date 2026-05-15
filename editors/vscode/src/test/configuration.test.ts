import * as assert from 'assert';
import { ConfigurationManager } from '../configuration';
import * as sinon from 'sinon';
import { expect } from 'chai';

describe('ConfigurationManager', () => {
    let adapter: any;
    let onChangeSpy: sinon.SinonSpy;
    let changeCallback: (e: any) => void;

    beforeEach(() => {
        adapter = {
            getConfiguration: sinon.stub(),
            onDidChangeConfiguration: sinon.stub().callsFake((cb: any) => {
                changeCallback = cb;
                return { dispose: () => { } };
            })
        };
        onChangeSpy = sinon.spy();
    });

    it('should trigger callback when controlPlaneAddr changes', () => {
        new ConfigurationManager(adapter, onChangeSpy);

        // Simulate event
        const event = {
            affectsConfiguration: (section: string) => section === 'heddle.controlPlaneAddr'
        };

        changeCallback(event);

        expect(onChangeSpy.called).to.be.true;
    });

    it('should trigger callback when lspPath changes', () => {
        new ConfigurationManager(adapter, onChangeSpy);

        // Simulate event
        const event = {
            affectsConfiguration: (section: string) => section === 'heddle.lspPath'
        };

        changeCallback(event);

        expect(onChangeSpy.called).to.be.true;
    });

    it('should NOT trigger callback when unrelated config changes', () => {
        new ConfigurationManager(adapter, onChangeSpy);

        const event = {
            affectsConfiguration: sinon.stub().returns(false)
        };

        changeCallback(event);

        expect(onChangeSpy.called).to.be.false;
    });
});
