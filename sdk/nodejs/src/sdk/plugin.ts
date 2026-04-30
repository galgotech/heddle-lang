import 'reflect-metadata';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { Table } from '../core/table';
import { ResourceState } from '../core/resource';
import { v4 as uuidv4 } from 'uuid';
import * as path from 'path';

export class HeddleBusinessError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'HeddleBusinessError';
    }
}

interface ResourceMetadata {
    name: string;
    target: any;
    methodName: string;
}

interface StepMetadata {
    name: string;
    resource?: string;
    target: any;
    methodName: string;
}

const RESOURCE_REGISTRY: Record<string, ResourceMetadata> = {};
const STEP_REGISTRY: Record<string, StepMetadata> = {};

export function Resource(options: { name: string }) {
    return function (target: any, propertyKey: string, descriptor: PropertyDescriptor) {
        RESOURCE_REGISTRY[options.name] = {
            name: options.name,
            target: target.constructor,
            methodName: propertyKey,
        };
    };
}

export function Step(options: { name: string; resource?: string }) {
    return function (target: any, propertyKey: string, descriptor: PropertyDescriptor) {
        STEP_REGISTRY[options.name] = {
            name: options.name,
            resource: options.resource,
            target: target.constructor,
            methodName: propertyKey,
        };
    };
}

export class PluginRegistry {
    private activeResources: Map<string, ResourceState> = new Map();
    private pluginInstances: Map<any, any> = new Map();
    private server: grpc.Server;

    constructor() {
        this.server = new grpc.Server();
    }

    private getPluginInstance(TargetClass: any): any {
        if (!this.pluginInstances.has(TargetClass)) {
            this.pluginInstances.set(TargetClass, new TargetClass());
        }
        return this.pluginInstances.get(TargetClass);
    }

    public async start(port: number = 50051) {
        const PROTO_PATH = path.resolve(__dirname, '../../proto/worker.proto');
        const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
        });

        const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any;
        const heddle = protoDescriptor.heddle;

        if (!heddle || !heddle.worker || !heddle.worker.PluginService) {
            throw new Error(`PluginService not found in proto definition.`);
        }

        this.server.addService(heddle.worker.PluginService.service, {
            InitResource: this.initResource.bind(this),
            ExecuteStep: this.executeStep.bind(this),
        });

        return new Promise<void>((resolve, reject) => {
            this.server.bindAsync(
                `0.0.0.0:${port}`,
                grpc.ServerCredentials.createInsecure(),
                (err, bindPort) => {
                    if (err) {
                        reject(err);
                    } else {
                        console.log(`Plugin SDK Server listening on port ${bindPort}`);
                        // Don't call this.server.start() anymore
                        resolve();
                    }
                }
            );
        });
    }

    private async initResource(
        call: grpc.ServerUnaryCall<any, any>,
        callback: grpc.sendUnaryData<any>
    ) {
        try {
            const req = call.request;
            const resourceMeta = RESOURCE_REGISTRY[req.resource_name];

            if (!resourceMeta) {
                throw new HeddleBusinessError(`Resource not found: ${req.resource_name}`);
            }

            const instance = this.getPluginInstance(resourceMeta.target);
            const method = instance[resourceMeta.methodName].bind(instance);

            let config = {};
            if (req.config_json) {
                try {
                    config = JSON.parse(req.config_json);
                } catch (e) {
                    throw new HeddleBusinessError(`Invalid config JSON`);
                }
            }

            const resourceState: ResourceState = await method(config);

            // Assume it has a start method if it extends ResourceState (optional call)
            if (resourceState && typeof resourceState.start === 'function') {
                await resourceState.start();
            }

            const resourceId = uuidv4();
            this.activeResources.set(resourceId, resourceState);

            callback(null, {
                status: 'SUCCESS',
                resource_id: resourceId,
                error_message: '',
            });
        } catch (error: any) {
            if (error instanceof HeddleBusinessError) {
                callback(null, {
                    status: 'BUSINESS_ERROR',
                    resource_id: '',
                    error_message: error.message,
                });
            } else {
                callback(null, {
                    status: 'FATAL_ERROR',
                    resource_id: '',
                    error_message: error.stack || error.message || 'Unknown Error',
                });
            }
        }
    }

    private async executeStep(
        call: grpc.ServerUnaryCall<any, any>,
        callback: grpc.sendUnaryData<any>
    ) {
        try {
            const req = call.request;
            const stepMeta = STEP_REGISTRY[req.step_name];

            if (!stepMeta) {
                throw new HeddleBusinessError(`Step not found: ${req.step_name}`);
            }

            const instance = this.getPluginInstance(stepMeta.target);
            const method = instance[stepMeta.methodName].bind(instance);

            let config: any = {};
            if (req.config_json) {
                try {
                    config = JSON.parse(req.config_json);
                } catch (e) {
                    throw new HeddleBusinessError(`Invalid config JSON`);
                }
            }

            // Dependency Injection
            if (stepMeta.resource) {
                if (!req.resource_id) {
                    throw new HeddleBusinessError(`Step requires resource '${stepMeta.resource}' but no resource_id was provided.`);
                }
                const activeResource = this.activeResources.get(req.resource_id);
                if (!activeResource) {
                    throw new HeddleBusinessError(`Active resource not found for id: ${req.resource_id}`);
                }
                config.resource = activeResource;
            }

            const inputTable = req.input_table ? Table.fromBuffer(req.input_table) : null;

            const outputTable: Table = await method(config, inputTable);

            callback(null, {
                status: 'SUCCESS',
                output_table: outputTable ? outputTable.toBuffer() : Buffer.alloc(0),
                error_message: '',
            });

        } catch (error: any) {
            if (error instanceof HeddleBusinessError) {
                callback(null, {
                    status: 'BUSINESS_ERROR',
                    output_table: Buffer.alloc(0),
                    error_message: error.message,
                });
            } else {
                callback(null, {
                    status: 'FATAL_ERROR',
                    output_table: Buffer.alloc(0),
                    error_message: error.stack || error.message || 'Unknown Error',
                });
            }
        }
    }
}
