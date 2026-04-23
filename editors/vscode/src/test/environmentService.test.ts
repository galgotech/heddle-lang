import * as assert from 'assert';
import * as path from 'path';
import * as fs from 'fs';
import * as sinon from 'sinon';
import * as cp from 'child_process';
import { EnvironmentService } from '../environmentService';

describe('EnvironmentService', () => {
    let sandbox: sinon.SinonSandbox;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
    });

    afterEach(() => {
        sandbox.restore();
    });

    it('should return poetry interpreter path when heddle-private exists with .venv', async () => {
        const workspaceFolder = '/home/user/project';
        const heddlePrivatePath = path.resolve(workspaceFolder, '../heddle-private');
        const venvPythonPath = path.join(heddlePrivatePath, '.venv', 'bin', 'python');

        const fsExistsStub = sandbox.stub(fs, 'existsSync');
        fsExistsStub.withArgs(heddlePrivatePath).returns(true);
        fsExistsStub.withArgs(path.join(heddlePrivatePath, '.venv')).returns(true);
        fsExistsStub.withArgs(venvPythonPath).returns(true);

        const service = new EnvironmentService();
        const result = await service.getPoetryInterpreterPath(workspaceFolder);

        assert.strictEqual(result, venvPythonPath);
    });

    it('should return undefined when heddle-private does not exist', async () => {
        const workspaceFolder = '/home/user/project';
        const heddlePrivatePath = path.resolve(workspaceFolder, '../heddle-private');

        const fsExistsStub = sandbox.stub(fs, 'existsSync');
        fsExistsStub.withArgs(heddlePrivatePath).returns(false);

        const service = new EnvironmentService();
        const result = await service.getPoetryInterpreterPath(workspaceFolder);

        assert.strictEqual(result, undefined);
    });

    it('should fallback to poetry env info when .venv is missing', async () => {
        const workspaceFolder = '/home/user/project';
        const heddlePrivatePath = path.resolve(workspaceFolder, '../heddle-private');
        const poetryEnvPath = '/path/to/virtualenv/bin/python';

        const fsExistsStub = sandbox.stub(fs, 'existsSync');
        fsExistsStub.withArgs(heddlePrivatePath).returns(true);
        fsExistsStub.withArgs(path.join(heddlePrivatePath, '.venv')).returns(false);

        const execStub = sandbox.stub(cp, 'exec');
        // Type casting for mock implementation of exec
        (execStub as any).callsFake((cmd: string, options: any, callback: any) => {
            if (cmd.includes('poetry env info --path')) {
                callback(null, '/path/to/virtualenv', '');
            } else {
                callback(new Error('Command failed'), '', '');
            }
        });
        
        // We assume logic will append /bin/python to the result of 'poetry env info --path'
        const expectedPath = path.join('/path/to/virtualenv', 'bin', 'python');

        const service = new EnvironmentService();
        const result = await service.getPoetryInterpreterPath(workspaceFolder);

        assert.strictEqual(result, expectedPath);
    });
});
