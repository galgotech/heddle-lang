import { PluginRegistry, Resource, Step, HeddleBusinessError } from './sdk/plugin';
import { ResourceConfig, ResourceState } from './core/resource';
import { StepConfig } from './core/step';
import { Table } from './core/table';

class ResourceConfigHttp extends ResourceConfig {
    host: string = "127.0.0.1";
    port: number = 8080;
}

class HttpResource extends ResourceState {
    config: ResourceConfigHttp;

    constructor(config: ResourceConfigHttp) {
        super();
        this.config = config;
    }

    async start(): Promise<void> {
        console.log(`Starting HTTP server on ${this.config.host}:${this.config.port}`);
    }

    async stop(): Promise<void> {
        console.log(`Stopping HTTP server on ${this.config.host}:${this.config.port}`);
    }
}

class StepConfigRoute extends StepConfig<ResourceConfigHttp> {
    path!: string;
    method!: string;
}

export class HttpPlugin {
    @Resource({ name: "http_server" })
    server(config: ResourceConfigHttp): HttpResource {
        return new HttpResource(config);
    }

    @Step({ name: "http_route", resource: "http_server" })
    route(config: StepConfigRoute, input: Table | null): Table {
        if (!config.resource) {
            throw new HeddleBusinessError("Resource not injected properly");
        }
        console.log(`Setting up route ${config.method} ${config.path}`);
        return new Table({ status: "success" });
    }
}

async function runTest() {
    console.log("Instantiated HttpPlugin, testing decorators...");
    const pluginRegistry = new PluginRegistry();
    console.log("Starting PluginRegistry...");
    await pluginRegistry.start(50051);
    console.log("Started PluginRegistry successfully.");
    process.exit(0);
}

runTest().catch(err => {
    console.error("Test failed:", err);
    process.exit(1);
});
