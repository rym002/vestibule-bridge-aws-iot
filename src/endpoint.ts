import { EndpointConnector } from '@vestibule-link/bridge-service-provider';
import { iotshadow, mqtt } from "aws-iot-device-sdk-v2";
import { ShadowState } from "aws-iot-device-sdk-v2/dist/iotshadow/model";
import { EventEmitter } from "events";
import { applyPatch, compare, RemoveOperation, ReplaceOperation } from 'fast-json-patch';
import { isEmpty } from "lodash";
import { basename, dirname } from 'path';
import { awsConnection, iotConfig } from "./iot";
import { v4 as uuid } from 'uuid'
interface NamedShadowRequest {
    shadowName: string;
    thingName: string;
}

export interface IotShadowEndpoint<ShadowType extends object> extends EndpointConnector {
    /**
     * Groups updates by deltaId by watching all promises for completion
     * Each promise is responsible to complete its update task
     * @param promise updated task to watch
     * @param deltaId id of related tasks
     */
    watchDeltaUpdate(promise: Promise<void>, deltaId: symbol): void
    /**
     * Indicates all changes have been sent.
     * Connector should update the shadow with the changes based on deltaId
     * @param deltaId change id
     */
    completeDeltaState(deltaId: symbol): Promise<void>;

    readonly reportedState: ShadowType
}

type VoidResolve = () => void
interface PromiseResolver {
    resolve: VoidResolve
    reject: (reason: any) => void
}
export abstract class AbstractIotShadowEndpoint<ShadowType extends object> extends EventEmitter implements IotShadowEndpoint<ShadowType> {
    protected readonly shadowClient: iotshadow.IotShadowClient
    protected readonly namedShadowRequest: NamedShadowRequest
    protected _reportedState: ShadowType = <any>{}
    private shadowVersion: number = 0
    private readonly deltaPromises = new Map<symbol, Promise<void>[]>()
    private readonly deltaEndpointsState = new Map<symbol, ShadowType>();
    private readonly tokenResolvers = new Map<string, PromiseResolver>()
    constructor(readonly endpointId: string) {
        super()
        const appConfig = iotConfig()
        const connection = awsConnection();
        this.shadowClient = new iotshadow.IotShadowClient(connection);
        this.namedShadowRequest = {
            shadowName: endpointId,
            thingName: appConfig.clientId
        }
        connection.on('resume',this.resumeConnection.bind(this))
    }

    get reportedState() {
        return this._reportedState
    }

    private async resumeConnection(returnCode:number,sessionPresent:boolean):Promise<void>{
        console.log('Resume Connection returnCode:%s session:%s', returnCode, sessionPresent)
        if (!sessionPresent){
            await this.subscribeMessages()
        }
    }
    public async subscribeMessages(): Promise<void> {
        const deleteAccepted = await this.shadowClient
            .subscribeToDeleteNamedShadowAccepted(this.namedShadowRequest,
                mqtt.QoS.AtLeastOnce,
                this.shadowDeleteAcceptedHandler.bind(this))
        this.verifyMqttSubscription(deleteAccepted)

        const shadowDeleteError = await this.shadowClient
            .subscribeToDeleteNamedShadowRejected(this.namedShadowRequest,
                mqtt.QoS.AtLeastOnce, this.shadowErrorHandler.bind(this))
        this.verifyMqttSubscription(shadowDeleteError)

        const shadowUpdateError = await this.shadowClient
            .subscribeToUpdateNamedShadowRejected(this.namedShadowRequest,
                mqtt.QoS.AtLeastOnce, this.shadowErrorHandler.bind(this))
        this.verifyMqttSubscription(shadowUpdateError)

        const shadowDelta = await this.shadowClient
            .subscribeToNamedShadowDeltaUpdatedEvents(this.namedShadowRequest,
                mqtt.QoS.AtLeastOnce, this.shadowDeltaHandler.bind(this))
        this.verifyMqttSubscription(shadowDelta)

        const shadowUpdated = await this.shadowClient
            .subscribeToNamedShadowUpdatedEvents(this.namedShadowRequest,
                mqtt.QoS.AtLeastOnce, this.shadowUpdatedHandler.bind(this))
        this.verifyMqttSubscription(shadowUpdated)

        const deleteShadow = await this.shadowClient
            .publishDeleteNamedShadow(this.namedShadowRequest, mqtt.QoS.AtLeastOnce)

    }

    protected verifyMqttSubscription(req: mqtt.MqttSubscribeRequest) {
        if (req.error_code) {
            const message = `Failed to subscibe to topic ${req.topic} error code ${req.error_code}`
            console.error(message)
            throw new Error(message)
        }
    }

    watchDeltaUpdate(promise: Promise<void>, deltaId: symbol) {
        let transPromises = this.deltaPromises.get(deltaId);
        if (!transPromises) {
            transPromises = [];
            this.deltaPromises.set(deltaId, transPromises);
        }
        transPromises.push(promise
            .catch((err) => {
                console.log(err)
            }));
    }

    /**
     * Waits for all promises to complete.
     * Used before making updates that depend on promises to complete
     * @param deltaId id of related tasks
     */
    protected async waitDeltaPromises(deltaId: symbol) {
        const promises = this.deltaPromises.get(deltaId);
        if (promises) {
            await Promise.all(promises);
            this.deltaPromises.delete(deltaId);
        }
    }

    protected abstract handleDeltaState(state: ShadowType): Promise<void>
    public abstract refresh(deltaId: symbol): Promise<void>
    private checkVersion(newVersion: number) {
        const ret = this.shadowVersion <= newVersion
        if (!ret) {
            console.error('Old Version received: %s Current Version: %s', newVersion, this.shadowVersion)
        }
        return ret
    }
    private async shadowDeltaHandler(error?: iotshadow.IotShadowError, response?: iotshadow.model.ShadowDeltaUpdatedEvent) {
        this.handleShadowError('Delta', error)
        if (response) {
            if (this.checkVersion(response.version)) {
                try {
                    await this.handleDeltaState(<ShadowType>response.state)
                } catch (err) {
                    console.error("%s error: %o response: %o", this.endpointId, err, response)
                }
            }
        }
    }


    private async shadowDeleteAcceptedHandler(error?: iotshadow.IotShadowError, response?: iotshadow.model.DeleteShadowResponse) {
        this.handleShadowError('Delete', error)
        if (response) {
            console.info("%s shadow deleted", this.endpointId)
            const deltaId = Symbol()
            await this.refresh(deltaId)
            this.shadowVersion = 0
        }
    }

    private async shadowUpdatedHandler(error?: iotshadow.IotShadowError, response?: iotshadow.model.ShadowUpdatedEvent) {
        this.handleShadowError('Update', error)
        if (response) {
            try {
                const current = response.current
                if (this.checkVersion(current.version)) {
                    this.shadowVersion = current.version
                    this._reportedState = <ShadowType>current.state.reported
                }
            } finally {
                const token: string | undefined = response['clientToken']
                const tokenResolver = this.tokenResolvers.get(token)
                if (tokenResolver) {
                    tokenResolver.resolve()
                }
            }

        }
    }

    private shadowErrorHandler(error?: iotshadow.IotShadowError, response?: iotshadow.model.ErrorResponse) {
        this.handleShadowError('Shadow Error', error)
        if (response) {
            console.error('%s Shadow error %o', this.endpointId, response)
            const clientToken = response.clientToken
            if (clientToken) {
                const tokenResolver = this.tokenResolvers.get(clientToken)
                if (tokenResolver) {
                    tokenResolver.reject(response)
                }
            }
        }
    }

    protected handleShadowError(errorType: string, error?: iotshadow.IotShadowError) {
        if (error) {
            console.error("%s error %o", errorType, error)
        }
    }

    protected async publishReportedState(stateUpdate?: ShadowType) {
        if (stateUpdate) {
            const state = this.createShadow(stateUpdate)
            if (!isEmpty(state.reported)) {
                const clientToken = uuid()
                let timeoutId: NodeJS.Timeout
                const shadowUpdate = new Promise<void>((resolve, reject) => {
                    this.tokenResolvers.set(clientToken, {
                        reject,
                        resolve
                    })
                    timeoutId = setTimeout(() => {
                        const resolver = this.tokenResolvers.get(clientToken)
                        if (resolver) {
                            resolver.reject(`Shadow update timeout. No document update received for clientToken:${clientToken}`)
                        }
                    }, iotConfig().shadowUpdateTimeout);
                })
                await this.shadowClient.publishUpdateNamedShadow({
                    ...this.namedShadowRequest,
                    state,
                    clientToken
                }, mqtt.QoS.AtLeastOnce)
                try {
                    await shadowUpdate
                    clearTimeout(timeoutId)
                } finally {
                    this.tokenResolvers.delete(clientToken)
                }
            } else {
                console.info('Shadow Reported not changed, skipping updated')
            }
        }
    }

    /**
     * 
     * @param state requested changes. This can be partial updates
     */
    protected createShadow(state: ShadowType): ShadowState {
        const deltaPatchResult = this.computeStateDiff(state);
        const reported = deltaPatchResult
        const desired = this.createDesired(reported)
        return {
            desired,
            reported
        }
    }

    private computeStateDiff(state: ShadowType): ShadowType {
        // Find the difference between current and updates
        const stateDiff = compare(this.reportedState, state);
        // Filter remove operation because state is a partial update
        const removeOperations = stateDiff
            .filter(operation => operation.op !== 'remove')
            .map((operation): RemoveOperation => {
                // Convert operation to remove. 
                const objName = basename(operation.path);
                const parsed = Number.parseInt(objName);
                if (!isNaN(parsed)) {
                    // Array member, use the array path
                    const parentName = dirname(operation.path);
                    return {
                        op: 'remove',
                        path: parentName
                    };
                } else {
                    return {
                        op: 'remove',
                        path: operation.path
                    };
                }

            });
        // Apply patch to remove all unmatched paths, left with matched in result
        const matchedPathsResult = applyPatch(state, removeOperations, false, false);
        // Create patch to remove all matches
        const matchedDiff = compare(matchedPathsResult.newDocument, {});

        // Filter operations that have a common parent. 
        // This prevents removing the parent when the child has a change
        const filteredMatch = matchedDiff.filter(match => {
            return stateDiff.map(stateOp => {
                return stateOp.path.startsWith(match.path);
            }).filter(value => value).length == 0;
        });
        // Remove current state matches from request
        const deltaPatchResult = applyPatch(state, filteredMatch, false, false);
        return deltaPatchResult.newDocument;
    }

    private createDesired(shadow: ShadowType): ShadowType | null {
        const diffs = compare({}, shadow)
        const nullOperations = diffs.map((operation): ReplaceOperation<null> => {
            return {
                op: 'replace',
                path: operation.path,
                value: null
            }
        })
        const patchResult = applyPatch(shadow, nullOperations, false, false)
        const ret = patchResult.newDocument
        return isEmpty(ret) ? null : ret
    }

    protected getDeltaEndpoint(deltaId: symbol) {
        let deltaEndpoint = this.deltaEndpointsState.get(deltaId);
        if (!deltaEndpoint) {
            deltaEndpoint = <any>{};
            this.deltaEndpointsState.set(deltaId, deltaEndpoint);
        }
        return deltaEndpoint;
    }
    async completeDeltaState(deltaId: symbol) {
        try {
            await this.waitDeltaPromises(deltaId);
            await this.publishReportedState(this.deltaEndpointsState.get(deltaId))
            this.deltaEndpointsState.delete(deltaId);
        } catch (err) {
            console.log('Delta error %o', err)
        }
    }
}