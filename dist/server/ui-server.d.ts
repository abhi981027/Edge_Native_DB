import { SyncEngine } from '../core/engine';
import { OfflineSimulator } from '../network/adapter';
import { HttpNetworkAdapter } from '../network/http-adapter';
import { UserRepository } from '../core/users';
import { MultiDeviceSimulator } from '../simulator/multi-device';
import { Logger } from '../sdk/logger';
export declare function createUIServer(port: number, engine: SyncEngine, simulator: OfflineSimulator, httpAdapter: HttpNetworkAdapter, users: UserRepository, upstreamUrl: string, multiSim?: MultiDeviceSimulator, logger?: Logger): {
    close: () => void;
};
//# sourceMappingURL=ui-server.d.ts.map