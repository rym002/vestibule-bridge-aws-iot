import { registerModule } from '@vestibule-link/bridge';
import { startModule as startServiceProvider } from '@vestibule-link/bridge-service-provider'
import { initConnection } from './iot';

export { IotShadowEndpoint } from './endpoint'
export { awsConnection } from './iot'
let moduleId: symbol | undefined;

export function startModule() {
    if (!moduleId) {
        moduleId = registerModule({
            name: 'gateway-aws',
            init: async () => {
                await initConnection();
            },
            depends: [
                startServiceProvider()
            ]
        })
    }
    return moduleId;
}

