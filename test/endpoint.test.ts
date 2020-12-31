import { mqtt } from 'aws-iot-device-sdk-v2'
import { expect } from 'chai'
import 'mocha'
import { createSandbox, match, SinonSandbox, SinonStubbedInstance, SinonStubbedMember, StubbableType } from 'sinon'
import { AbstractIotShadowEndpoint } from '../src'
import * as iot from '../src/iot'


class EndpointTest extends AbstractIotShadowEndpoint<{}>{
    constructor(initalShadow: {}, endpointId: string) {
        super(endpointId)
        this._reportedState = initalShadow
    }
    public async handleDeltaState(state: {}): Promise<void> {
    }
    public async refresh(deltaId: symbol): Promise<void> {
    }

    public testShadow(state: {}) {
        return this.createShadow(state)
    }
}

interface TopicHandlerMap {
    [index: string]: (topic: string, payload: ArrayBuffer) => void | Promise<void>
}
const encoder = new TextEncoder()

async function emitTopic(topicHandlerMap: TopicHandlerMap, listenTopic: string, topic: string, req: any) {
    const topicHandler = topicHandlerMap[listenTopic]
    await topicHandler(topic, encoder.encode(JSON.stringify(req)))
}

describe('Endpoint test', () => {
    const clientId = 'testClientId'
    before(async function () {
        process.env.AWS_CLIENT_ID = clientId
    })
    beforeEach(function () {
        const sandbox = createSandbox()
        const topicHandlerMap: TopicHandlerMap = {}

        const connectionStub = createSinonStubInstance(sandbox, mqtt.MqttClientConnection)
        connectionStub.publish.returns(Promise.resolve({}))
        connectionStub.subscribe.callsFake((topic, qos, on_message) => {
            topicHandlerMap[topic] = on_message
            return Promise.resolve({
                topic: topic,
                qos: qos
            })
        })
        const createConnectionStub = sandbox.stub(iot, 'awsConnection').returns(connectionStub)
        this.currentTest['sandbox'] = sandbox
        this.currentTest['topicHandlerMap'] = topicHandlerMap
        this.currentTest['connection'] = connectionStub
    })
    afterEach(function () {
        this.currentTest['sandbox'].restore()
    })

    context('Shadow Subscriptions', function () {
        it('should call handleDeltaState when shadow delta', async function () {
            const topicHandlerMap = this.test['topicHandlerMap']
            const sandbox: SinonSandbox = this.test['sandbox']
            const inputState = {
                delta: true
            }
            const endpointId = 'handleDeltaState'
            const topic = `$aws/things/${clientId}/shadow/name/${endpointId}/update/delta`

            const instance = new EndpointTest({}, endpointId)
            const spy = sandbox.spy(instance, 'handleDeltaState')
            await instance.subscribeMessages()
            await emitTopic(
                topicHandlerMap,
                topic,
                topic,
                {
                    version: 1,
                    state: inputState
                })
            sandbox.assert.calledWith(spy, inputState)
        })

        it('should not call handleDeltaState with a past version', async function () {
            const topicHandlerMap = this.test['topicHandlerMap']
            const sandbox: SinonSandbox = this.test['sandbox']
            const inputState = {
                delta: true
            }
            const endpointId = 'handleDeltaStateOld'
            const topic = `$aws/things/${clientId}/shadow/name/${endpointId}/update/delta`

            const instance = new EndpointTest({}, endpointId)
            const spy = sandbox.spy(instance, 'handleDeltaState')
            await instance.subscribeMessages()
            await emitTopic(
                topicHandlerMap,
                topic,
                topic,
                {
                    version: -1,
                    state: inputState
                })
            sandbox.assert.notCalled(spy)
        })
        it('should call refreshState when shadow deleted', async function () {
            const topicHandlerMap = this.test['topicHandlerMap']
            const sandbox: SinonSandbox = this.test['sandbox']
            const endpointId = 'refreshState'
            const topic = `$aws/things/${clientId}/shadow/name/${endpointId}/delete/accepted`
            const instance = new EndpointTest({}, endpointId)
            const spy = sandbox.spy(instance, 'refresh')
            await instance.subscribeMessages()
            await emitTopic(
                topicHandlerMap,
                topic,
                topic,
                {
                })
            sandbox.assert.called(spy)
        })
        it('should update remoteShadow when shadow update accepted', async function () {
            const topicHandlerMap = this.test['topicHandlerMap']
            const endpointId = 'updateShadow'
            const topic = `$aws/things/${clientId}/shadow/name/${endpointId}/update/accepted`
            const inputState = {
                delta: true
            }
            const instance = new EndpointTest({}, endpointId)
            await instance.subscribeMessages()
            await emitTopic(
                topicHandlerMap,
                topic,
                topic,
                {
                    version: 2,
                    state: {
                        reported: inputState
                    }
                })
            expect(instance.reportedState).to.eql(inputState)
        })
        it('should publish delete shadow', async function () {
            const sandbox: SinonSandbox = this.test['sandbox']
            const connection: StubbedClass<mqtt.MqttClientConnection> = this.test['connection']
            const endpointId = 'deleteShadow'
            const topic = `$aws/things/${clientId}/shadow/name/${endpointId}/delete`
            const instance = new EndpointTest({}, endpointId)
            await instance.subscribeMessages()
            sandbox.assert.calledWithMatch(connection.publish, topic, match.any, mqtt.QoS.AtLeastOnce)
        })
    })
    context('Shadow Creation', function () {
        const o1 = {
            'b1': true,
            'n1': 123,
            's1': 's1v',
            'a1': [
                123
            ],
            'o1': {
                's11': 's11v'
            }
        }

        it('should diff booleans', () => {

            const o2 = {
                'b1': false,
                'n1': 123,
                's1': 's1v'
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.have.property('b1')
                .to.eql(false)
            expect(reported)
                .to.not.have.property('n1')
            expect(reported)
                .to.not.have.property('s1')
            expect(reported)
                .to.not.have.property('a1')
            expect(reported)
                .to.not.have.property('o1')
            expect(desired)
                .to.have.property('b1')
                .to.be.null
            expect(desired)
                .to.not.have.property('n1')
            expect(desired)
                .to.not.have.property('s1')
            expect(desired)
                .to.not.have.property('a1')
            expect(desired)
                .to.not.have.property('o1')
        })
        it('should diff numbers', () => {
            const o2 = {
                'b1': true,
                'n1': 456,
                's1': 's1v'
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.have.property('n1')
                .to.eql(456)
            expect(reported)
                .to.not.have.property('b1')
            expect(reported)
                .to.not.have.property('s1')
            expect(reported)
                .to.not.have.property('a1')
            expect(reported)
                .to.not.have.property('o1')
            expect(desired)
                .to.have.property('n1')
                .to.be.null
            expect(desired)
                .to.not.have.property('b1')
            expect(desired)
                .to.not.have.property('s1')
            expect(desired)
                .to.not.have.property('a1')
            expect(desired)
                .to.not.have.property('o1')
        })
        it('should diff strings', () => {
            const o2 = {
                'b1': true,
                'n1': 123,
                's1': 'newvalue'
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.have.property('s1')
                .to.eql('newvalue')
            expect(reported)
                .to.not.have.property('b1')
            expect(reported)
                .to.not.have.property('n1')
            expect(reported)
                .to.not.have.property('a1')
            expect(reported)
                .to.not.have.property('o1')
        })

        it('should ignore like arrays', () => {
            const o2 = {
                'b1': true,
                'n1': 123,
                's1': 's1v',
                's2': 'string',
                'a1': [
                    123
                ]
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.have.property('s2')
                .to.eql('string')
            expect(reported)
                .to.not.have.property('s1')
            expect(reported)
                .to.not.have.property('b1')
            expect(reported)
                .to.not.have.property('n1')
            expect(reported)
                .to.not.have.property('a1')
            expect(reported)
                .to.not.have.property('o1')
            expect(desired)
                .to.have.property('s2')
                .to.be.null
            expect(desired)
                .to.not.have.property('s1')
            expect(desired)
                .to.not.have.property('b1')
            expect(desired)
                .to.not.have.property('n1')
            expect(desired)
                .to.not.have.property('a1')
            expect(desired)
                .to.not.have.property('o1')
        })
        it('should return changed arrays', () => {
            const o2 = {
                'b1': true,
                'n1': 123,
                's1': 's1v',
                'a1': [
                    123,
                    456
                ]
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.have.property('a1')
                .to.have.length(2)
            expect(reported)
                .to.not.have.property('s1')
            expect(reported)
                .to.not.have.property('b1')
            expect(reported)
                .to.not.have.property('n1')
            expect(reported)
                .to.not.have.property('o1')
            expect(desired)
                .to.have.property('a1')
                .to.be.null
            expect(desired)
                .to.not.have.property('s1')
            expect(desired)
                .to.not.have.property('b1')
            expect(desired)
                .to.not.have.property('n1')
            expect(desired)
                .to.not.have.property('o1')
        })

        it('should return undefined', () => {
            const o2 = {
                'b1': true,
                'n1': 123,
                's1': undefined,
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.be.undefined
            expect(desired)
                .to.be.null
        })
        it('should return null', () => {
            const o2 = {
                'b1': true,
                'n1': 123,
                's1': null,
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.have.property('s1')
                .to.be.null
            expect(reported)
                .to.not.have.property('b1')
            expect(reported)
                .to.not.have.property('n1')
            expect(reported)
                .to.not.have.property('a1')
            expect(reported)
                .to.not.have.property('o1')
            expect(desired)
                .to.have.property('s1')
                .to.be.null
            expect(desired)
                .to.not.have.property('b1')
            expect(desired)
                .to.not.have.property('n1')
            expect(desired)
                .to.not.have.property('a1')
            expect(desired)
                .to.not.have.property('o1')
        })
        it('should return nested diff', () => {
            const o2 = {
                'o1': {
                    's11': 'newvalue'
                },
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.have.property('o1')
                .to.eql({
                    's11': 'newvalue'
                })
            expect(reported)
                .to.not.have.property('b1')
            expect(reported)
                .to.not.have.property('n1')
            expect(reported)
                .to.not.have.property('a1')
            expect(desired)
                .to.have.property('o1')
                .to.be.null
            expect(desired)
                .to.not.have.property('b1')
            expect(desired)
                .to.not.have.property('n1')
            expect(desired)
                .to.not.have.property('a1')
        })
        it('should return undefined on nested match', () => {
            const o2 = {
                'o1': {
                    's11': 's11v'
                },
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.be.undefined
            expect(desired)
                .to.be.null
        })
        it('should return if not defined in state', () => {
            const o2 = {
                'o1': {
                    's12': 's11v'
                },
            }
            const tester = new EndpointTest({}, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.eql(o2)
            expect(desired)
                .to.have.property('o1')
                .to.be.null
        })
        it('should return null nested object', () => {
            const o2 = {
                'o1': null,
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.have.property('o1')
                .to.be.null
            expect(desired)
                .to.have.property('o1')
                .to.be.null
        })
        it('should return undefined nested object if nested object is undefined', () => {
            const o2 = {
                'o1': undefined,
            }
            const tester = new EndpointTest(o1, 'state')
            const shadow = tester.testShadow(o2)
            const reported = shadow.reported
            const desired = shadow.desired
            expect(reported)
                .to.be.undefined
            expect(desired)
                .to.be.null
        })
    })
})

type StubbedClass<T> = SinonStubbedInstance<T> & T;
function createSinonStubInstance<T>(
    sandbox: SinonSandbox,
    constructor: StubbableType<T>,
    overrides?: { [K in keyof T]?: SinonStubbedMember<T[K]> },
): StubbedClass<T> {
    const stub = sandbox.createStubInstance<T>(constructor, overrides);
    return stub as unknown as StubbedClass<T>;
}
