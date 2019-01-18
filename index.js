var Kafka = require('kafka-node')
var Kefir = require('kefir')
module.exports = topic =>
    Kefir.stream(emitter => {
        var client = new Kafka.KafkaClient({kafkaHost})
        client.on('error', () => emitter.end())
        client.on('ready', () => client.createTopics([topic], (error, result) => error ? emitter.end() : () =>
                new Kafka.Offset(client).fetch([{ topic, time: -1}], (err, offsets_per_partitions) => {
                    var offset = offsets_per_partitions[topic]['0'][0] - 1
                    var consumer = new Kafka.Consumer(client, [{topic, offset}], {fromOffset: true, groupId: String(Date.now()) + Math.random()})
                    consumer.on('error',  () => emitter.end())
                    consumer.on('message', message => emitter.emit(message.value))
                })
            )
        )
    })
