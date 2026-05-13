import * as assert from 'assert';
import * as sinon from 'sinon';
import { expect } from 'chai';
import { SdkPluginsTreeDataProvider } from '../treeViews';
import { ProcessManager } from '../processManager';
import * as vscode from 'vscode';

describe('SdkPluginsTreeDataProvider', () => {
    let mockProcessManager: sinon.SinonStubbedInstance<ProcessManager>;
    let mockContext: any;
    let mockGlobalState: any;

    beforeEach(() => {
        mockProcessManager = {
            onStatusChange: sinon.stub().returns({ dispose: () => {} }),
            isRunning: sinon.stub().returns(false),
            start: sinon.stub().resolves(),
            stop: sinon.stub(),
            stopAll: sinon.stub(),
            onStatusChangeCall: (cb: any) => { return { dispose: () => {} }; }
        } as any;
        
        mockGlobalState = {
            get: sinon.stub().returns([]),
            update: sinon.stub().resolves()
        };

        mockContext = {
            globalState: mockGlobalState
        };
    });

    it('should add a folder and refresh', () => {
        const provider = new SdkPluginsTreeDataProvider(mockProcessManager as any, mockContext);
        const refreshSpy = sinon.spy(provider, 'refresh');

        provider.addFolder('/path/to/plugin');

        expect(mockGlobalState.update.calledWith('heddle.pluginFolders', ['/path/to/plugin'])).to.be.true;
        expect(refreshSpy.calledOnce).to.be.true;
    });

    it('should remove a folder and refresh', () => {
        mockGlobalState.get.withArgs('heddle.pluginFolders', []).returns(['/path/to/plugin']);
        const provider = new SdkPluginsTreeDataProvider(mockProcessManager as any, mockContext);
        const refreshSpy = sinon.spy(provider, 'refresh');

        provider.removeFolder('/path/to/plugin');

        expect(mockGlobalState.update.calledWith('heddle.pluginFolders', [])).to.be.true;
        expect(refreshSpy.calledOnce).to.be.true;
    });
});
