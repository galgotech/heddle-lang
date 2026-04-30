import { ResourceConfig, ResourceState } from './resource';

export abstract class StepConfig<R extends ResourceConfig | undefined = undefined> {
    resource?: R;
    _resourceState?: ResourceState;
}
