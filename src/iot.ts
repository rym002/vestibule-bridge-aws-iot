import { io, iot, mqtt } from "aws-iot-device-sdk-v2";


export function iotConfig() {
    return {
        certPath: process.env['AWS_CERT_PATH'],
        keyPath: process.env['AWS_KEY_PATH'],
        caDirPath: process.env['AWS_CA_DIR_PATH'],
        caFilePath: process.env['AWS_CA_FILE_PATH'],
        clientId: process.env['AWS_CLIENT_ID'],
        endpoint: process.env['AWS_IOT_ENDPOINT'],
        mqttLogLevel: process.env['AWS_MQTT_LOG_LEVEL']
    }

}

let connection:mqtt.MqttClientConnection|undefined

export async function initConnection(){
    connection = await createConnection()
}

export function awsConnection(){
    if (connection){
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
    //TODO expand to support all props
    const config = iot.AwsIotMqttConnectionConfigBuilder
        .new_mtls_builder_from_path(appConfig.certPath, appConfig.keyPath)
        .with_certificate_authority_from_path(appConfig.caDirPath, appConfig.caFilePath)
        .with_client_id(appConfig.clientId)
        .with_clean_session(true)
        .with_endpoint(appConfig.endpoint)
        .build()

    const connection = client.new_connection(config)

    //Timer is needed or else connect hangs and does not return
    const timer = setTimeout(() => { }, 60 * 1000);
    const connected = await connection.connect()
    clearTimeout(timer)

    if (!connected) {
        throw new Error("Connection failed")
    }

    return connection;
}