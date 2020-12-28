import { registerModule } from '@vestibule-link/bridge';
import { initConnection } from './iot';

export { IotShadowEndpoint } from './endpoint'

let moduleId: symbol | undefined;

export function startModule() {
    if (!moduleId) {
        moduleId = registerModule({
            name: 'gateway-aws',
            init: async () => {
                await initConnection();
            }
        })
    }
    return moduleId;
}

