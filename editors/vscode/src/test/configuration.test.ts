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
                return { dispose: () => {} };
            })
        };
        onChangeSpy = sinon.spy();
    });

    it('should trigger callback when pythonPath changes', () => {
        new ConfigurationManager(adapter, onChangeSpy);
        
        // Mock getConfiguration
        const configMock = {
            get: sinon.stub().returns('new/python/path')
        };
        adapter.getConfiguration.returns(configMock);

        // Simulate event
        const event = {
            affectsConfiguration: sinon.stub().returns(true)
        };
        
        changeCallback(event);

        expect(adapter.getConfiguration.calledWith('heddle')).to.be.true;
        expect(event.affectsConfiguration.calledWith('heddle.pythonPath')).to.be.true;
        expect(onChangeSpy.calledWith('new/python/path')).to.be.true;
    });

    it('should NOT trigger callback when other config changes', () => {
        new ConfigurationManager(adapter, onChangeSpy);
        
        const event = {
            affectsConfiguration: sinon.stub().returns(false)
        };
        
        changeCallback(event);

        expect(onChangeSpy.called).to.be.false;
    });

    it('should prioritize detected environment path over configuration', async () => {
        const detectedPath = '/path/to/poetry/python';
        const configPath = 'configured/python';
        
        const envServiceMock = {
            getPoetryInterpreterPath: sinon.stub().resolves(detectedPath)
        };

        const configMock = {
            get: sinon.stub().returns(configPath)
        };
        adapter.getConfiguration.returns(configMock);

        const manager = new ConfigurationManager(adapter, onChangeSpy, envServiceMock as any);
        const result = await manager.getPythonPath('/workspace');

        expect(result).to.equal(detectedPath);
    });

    it('should fallback to configuration if no environment detected', async () => {
        const configPath = 'configured/python';
        
        const envServiceMock = {
            getPoetryInterpreterPath: sinon.stub().resolves(undefined)
        };

        const configMock = {
            get: sinon.stub().returns(configPath)
        };
        adapter.getConfiguration.returns(configMock);

        const manager = new ConfigurationManager(adapter, onChangeSpy, envServiceMock as any);
        const result = await manager.getPythonPath('/workspace');

        expect(result).to.equal(configPath);
    });
});
