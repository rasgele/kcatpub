#!/usr/bin/env node
const {Kafka, Partitioners} = require('kafkajs')
const readline = require('readline');
const { program } = require('commander');

function parseCommandline() {
    program.option('-b, --broker-url <url>', 'Kafka broker URL', 'localhost:29092')
        .requiredOption('-f, --file <file>', 'input file')
        .requiredOption('-t, --topic <topic>', 'topic to publish to')
        .option('-x, --batch-size <batchSize>', 'number of messages', 10000)
        .version('0.0.1')
        .parse();
    return program.opts();
}
async function run(programOptions) {
    const options = {
        brokers: [programOptions.brokerUrl],
        clientId: 'kcatpub-producer',
        input: programOptions.file,
        topic: programOptions.topic,
        maxBatch: programOptions.batchSize,
    };

    console.log(`Running with options:\n ${JSON.stringify(options, null, 2)}`);
    const kafka = new Kafka({
        clientId: options.clientId,
        brokers: options.brokers,
    })
    const producer = kafka.producer({createPartitioner: Partitioners.DefaultPartitioner})
    await producer.connect();

    const rl = readline.createInterface({
        input: require('fs').createReadStream(options.input)
    });

    let batch = [];

    const sendMessagesAsync = async (messages) => {
        console.log(`Sending message batch of size ${messages.length}.`);
        const metadata = await producer.send({ topic: options.topic, messages});
        console.log(`Sent message batch of size ${messages.length}.`);
    }

    let totalMessages = 0;
    let maxMessageSize = 0;

    for await (const line of rl) {
        maxMessageSize = maxMessageSize < line.length ? line.length : maxMessageSize;
        const event = JSON.parse(line);

        const headersArray = event.headers ?? [];
        const headers = {};
        for (let i = 0; i < headersArray.length; i++) {
            const key = headersArray[i];
            i++;
            headers[`${key}`] = headersArray[i];
        }

        batch.push({
            headers,
            value: event.payload == null ? null : JSON.stringify(event.payload)
        });

        if (batch.length >= options.maxBatch) {
            console.log(`Max size is ${maxMessageSize}.`);
            await sendMessagesAsync(batch);
            totalMessages += batch.length;
            batch = [];
        }
    }

    if (batch.length > 0) {
        await sendMessagesAsync(batch);
        totalMessages += batch.length;
    }
    console.log(`Totally sent ${totalMessages} messages.`);

    await producer.disconnect();
    console.log(`Disconnected`);
}

const opts = parseCommandline();
run(opts).finally(() => console.log("That's all!"));
