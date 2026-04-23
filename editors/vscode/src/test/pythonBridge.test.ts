import * as assert from 'assert';
import { PythonBridge } from '../pythonBridge';
import * as sinon from 'sinon';
import * as cp from 'child_process';
import { expect } from 'chai';

describe('PythonBridge', () => {
    let execFileStub: sinon.SinonStub;

    beforeEach(() => {
        execFileStub = sinon.stub(cp, 'execFile');
    });

    afterEach(() => {
        execFileStub.restore();
    });

    it('should return heddle version when installed', async () => {
        execFileStub.yields(null, 'heddle 0.1.0\n', '');

        const bridge = new PythonBridge('python');
        const version = await bridge.getHeddleVersion();
        
        expect(version).to.equal('heddle 0.1.0');
        // We expect it to call `python -m heddle --version`
        expect(execFileStub.calledWith('python', ['-m', 'heddle', '--version'])).to.be.true;
    });

    it('should throw error when heddle is not installed', async () => {
        const error = new Error('Command failed');
        execFileStub.yields(error, '', 'No module named heddle');

        const bridge = new PythonBridge('python');
        
        try {
            await bridge.getHeddleVersion();
            assert.fail('Should have thrown an error');
        } catch (e: any) {
            expect(e.message).to.include('Command failed');
        }
    });

    it('should execute generic heddle commands', async () => {
        execFileStub.yields(null, '{"some": "json"}', '');
        
        const bridge = new PythonBridge('python');
        const result = await bridge.executeCommand(['inspect']);
        
        expect(result).to.equal('{"some": "json"}');
        expect(execFileStub.calledWith('python', ['-m', 'heddle', 'inspect'])).to.be.true;
    });

    it('should handle composite commands like poetry run python', async () => {
        execFileStub.yields(null, 'output', '');
        
        // Use a space but ensure it's not an existing file (mocked by fs in implementation)
        const bridge = new PythonBridge('poetry run python');
        await bridge.executeCommand(['run']);
        
        expect(execFileStub.calledWith('poetry', ['run', 'python', '-m', 'heddle', 'run'])).to.be.true;
    });
});
