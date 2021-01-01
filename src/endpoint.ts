import { iotshadow, mqtt } from "aws-iot-device-sdk-v2";
import { ShadowState } from "aws-iot-device-sdk-v2/dist/iotshadow/model";
import { EventEmitter } from "events";
import { isArray, isEmpty, isEqual, isObject, mapValues, pickBy } from "lodash";
import { iotConfig, awsConnection } from "./iot";
import { EndpointConnector } from '@vestibule-link/bridge-service-provider'
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
export abstract class AbstractIotShadowEndpoint<ShadowType extends object> extends EventEmitter implements IotShadowEndpoint<ShadowType> {
    protected readonly shadowClient: iotshadow.IotShadowClient
    protected readonly namedShadowRequest: NamedShadowRequest
    protected _reportedState: ShadowType = <any>{}
    private shadowVersion: number = 0
    private readonly deltaPromises = new Map<symbol, Promise<void>[]>()
    private readonly deltaEndpointsState = new Map<symbol, ShadowType>();
    constructor(readonly endpointId: string) {
        super()
        const appConfig = iotConfig()
        this.shadowClient = new iotshadow.IotShadowClient(awsConnection());
        this.namedShadowRequest = {
            shadowName: endpointId,
            thingName: appConfig.clientId
        }
    }

    get reportedState() {
        return this._reportedState
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

        const shadowUpdateAccepted = await this.shadowClient
            .subscribeToUpdateNamedShadowAccepted(this.namedShadowRequest,
                mqtt.QoS.AtLeastOnce, this.shadowUpdateAcceptedHandler.bind(this))
        this.verifyMqttSubscription(shadowUpdateAccepted)

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

    private async shadowUpdateAcceptedHandler(error?: iotshadow.IotShadowError, response?: iotshadow.model.UpdateShadowResponse) {
        this.handleShadowError('Update', error)
        if (response) {
            if (this.checkVersion(response.version)) {
                this.shadowVersion = response.version
                this._reportedState = <ShadowType>response.state.reported
            }
        }
    }

    private shadowErrorHandler(error?: iotshadow.IotShadowError, response?: iotshadow.model.ErrorResponse) {
        this.handleShadowError('Shadow Error', error)
        if (response) {
            console.error('%s Shadow error %o', this.endpointId, response)
        }
    }

    protected handleShadowError(errorType: string, error?: iotshadow.IotShadowError) {
        if (error) {
            console.error("%s error %o", errorType, error)
        }
    }

    protected publishReportedState(state: ShadowType) {
        const shadow = this.createShadow(state)
        if (shadow.reported) {
            this.shadowClient.publishUpdateNamedShadow({
                ...this.namedShadowRequest,
                state: shadow
            }, mqtt.QoS.AtLeastOnce)
        } else {
            console.info('Shadow Reported not changed, skipping updated')
        }
    }

    protected createShadow(state: ShadowType): ShadowState {
        const reported = this.diffObject(state, this.reportedState)
        const desired = this.mapDesiredObject(reported, true)
        return {
            desired: desired,
            reported: reported
        }
    }

    private diffObject<T extends object>(newState: T, currentState: T): T | undefined {
        if (currentState == undefined) {
            return newState
        } else if (!isEqual(newState, currentState)) {
            const mapped = mapValues(newState, (newValue, key) => {
                const currentValue = currentState[key]
                if (!isEqual(newValue, currentValue)) {
                    if (!isArray(newValue) && isObject(newValue)) {
                        const childObj = this.diffObject(newValue, currentValue)
                        if (!isEmpty(childObj)) {
                            return childObj
                        } else {
                            return undefined
                        }
                    } else {
                        return newValue
                    }
                } else {
                    return undefined
                }
            })
            const filtered = pickBy(mapped, (value) => {
                return value !== undefined
            })
            return isEmpty(filtered) ? undefined : <T>filtered
        } else {
            return undefined
        }
    }
    private mapDesiredObject(state: object, rootObject: boolean): object | null {
        const mapped = mapValues(state, (value, key) => {
            if (isObject(value)) {
                return this.mapDesiredObject(value, false)
            } else if (isArray(value)) {
                return this.mapDesiredArray(value)
            } else {
                return null
            }
        })
        const filtered = rootObject ? mapped : pickBy(mapped, (value) => {
            return value !== null
        })

        return isEmpty(filtered) ? null : filtered
    }
    private mapDesiredArray(state: any[]): any[] | null {
        const mapped = state.map(value => {
            if (isObject(value)) {
                return this.mapDesiredObject(value, false)
            } else if (isArray(value)) {
                return this.mapDesiredArray(value)
            } else {
                return null;
            }
        }).filter(value => value !== null)
        return isEmpty(mapped) ? mapped : null
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
        await this.waitDeltaPromises(deltaId);
        this.publishReportedState(this.deltaEndpointsState.get(deltaId))
        this.deltaEndpointsState.delete(deltaId);
    }
}