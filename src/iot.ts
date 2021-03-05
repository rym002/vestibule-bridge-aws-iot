import { io, iot, mqtt } from "aws-iot-device-sdk-v2";


export function iotConfig() {
    return {
        certPath: process.env['AWS_CERT_PATH'],
        keyPath: process.env['AWS_KEY_PATH'],
        caDirPath: process.env['AWS_CA_DIR_PATH'],
        caFilePath: process.env['AWS_CA_FILE_PATH'],
        clientId: process.env['AWS_CLIENT_ID'],
        endpoint: process.env['AWS_IOT_ENDPOINT'],
        mqttLogLevel: process.env['AWS_MQTT_LOG_LEVEL'],
        mqttKeepAlive: Number(process.env['AWS_MQTT_KEEP_ALIVE']) | 120,
        shadowUpdateTimeout: process.env['AWS_SHADOW_UPDATE_TIMEOUT'] ? Number(process.env['AWS_SHADOW_UPDATE_TIMEOUT']) : 1000
    }

}

let connection: mqtt.MqttClientConnection | undefined

export async function initConnection() {
    connection = await createConnection()
}

export function awsConnection() {
    if (connection) {
        return connection
    }
    throw new Error('Connection not initalized')
}

async function createConnection(): Promise<mqtt.MqttClientConnection> {
    const appConfig = iotConfig()

    if (appConfig.mqttLogLevel) {
        const level: io.LogLevel = parseInt(io.LogLevel[appConfig.mqttLogLevel.toUpperCase()]);
        io.enable_logging(level);
    }

    const bootstrap = new io.ClientBootstrap()
    const client = new mqtt.MqttClient(bootstrap)
    const config = iot.AwsIotMqttConnectionConfigBuilder
        .new_mtls_builder_from_path(appConfig.certPath, appConfig.keyPath)
        .with_certificate_authority_from_path(appConfig.caDirPath, appConfig.caFilePath)
        .with_client_id(appConfig.clientId)
        .with_keep_alive_seconds(appConfig.mqttKeepAlive)
        .with_endpoint(appConfig.endpoint)
        .build()

    const connection = client.new_connection(config)

    connection.on('interrupt', (error) => {
        console.log('Connection Interrupt %j', error)
    })
    connection.on('connect', (sessionPresent) => {
        console.log('connected with session:%s', sessionPresent)
    })
    //Timer is needed or else connect hangs and does not return
    const timer = setTimeout(() => { }, 60 * 1000);
    const session = await connection.connect()
    clearTimeout(timer)
    console.info(`Created AWS IOT Connection with session ${session}`)

    return connection;
}