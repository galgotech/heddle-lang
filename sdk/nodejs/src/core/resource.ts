export abstract class ResourceConfig {
    // Base resource configuration
}

export abstract class ResourceState {
    abstract start(): Promise<void>;
    abstract stop(): Promise<void>;
}
