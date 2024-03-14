const fs = require('fs');
const { Kafka, Partitioners } = require('kafkajs');
const readline = require('readline');
const { program } = require('commander');

function parseCommandline() {
    program
        .requiredOption('-f, --file <file>', 'input file')
        .requiredOption('-t, --topic <topic>', 'topic to publish to')
        .option('-b, --broker-url <url>', 'Kafka broker URL', 'localhost:29092')
        .option('-x, --batch-size <batchSize>', 'number of messages', 10000)
        .option('-s, --session', 'use session', false)
        .version('1.0.0')
        .parse();
    return program.opts();
}

async function saveSession(offset, options) {
    await fs.promises.writeFile('session.json', JSON.stringify({startIndex: offset, options}), { encoding: 'utf8' });
    console.log(`Successfully saved the latest offset (${offset}) to session.`);
    console.log(`To continue from where it left off, run the program again with the same parameters:`);
    console.log(`kcatpub -f ${options.input} -t ${options.topic} -b ${options.brokers} -x ${options.maxBatch} -s`);
}

async function getSessionOffset() {
    try {
        if (fs.existsSync(sessionFile)) {
            const fileContent = await fs.promises.readFile(sessionFile, { encoding: 'utf8' });
            const sessionSettings = JSON.parse(fileContent);
            const offset = sessionSettings.startIndex;
            return isNaN(offset) ? 0 : offset;
        }
    } catch (err) {
        console.error(`Error reading session file: ${err}`);
        throw err;
    }
    return 0;
}

async function run(options) {
    console.log(`Running with options:\n ${JSON.stringify(options, null, 2)}`);

    const kafka = new Kafka({
        clientId: options.clientId,
        brokers: options.brokers,
    });

    producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
    await producer.connect();

    const sessionOffset = options.session ? await getSessionOffset() : 0;

    const rl = readline.createInterface({ input: fs.createReadStream(options.input) });

    let batch = [];

    const sendMessagesAsync = async (messages) => {
        try {
            console.log(`Sending message batch of size ${messages.length}.`);
            await producer.send({ topic: options.topic, messages });
            console.log(`Sent message batch of size ${messages.length}.`);
            linesProcessed += messages.length; // Update linesProcessed after successful send
        } catch (error) {
            await saveSession(linesProcessed, options);
            throw error; // Propagate the error
        }
    };

    try {
        for await (const line of rl) {
            if (shutdown) {
                break;
            }

            if (linesProcessed < sessionOffset) {
                linesProcessed++;
                continue;
            }
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
                lastSendPromise = sendMessagesAsync(batch);
                await lastSendPromise;
                batch = [];
            }
        }

        if (batch.length > 0) {
            await sendMessagesAsync(batch);
        }
    } catch (error) {
        console.error(`An error occurred: ${error.message}`);
    } finally {
        await producer.disconnect();
        console.log(`Done with publishing. Exiting.`);
    }
}

async function gracefulShutdown() {
    console.log('Graceful shutdown initiated.');
    try {
        shutdown = true;
        await lastSendPromise;
        console.log('Last send promise resolved');
        await producer.disconnect();
    } catch (err) {
        console.error(`Error saving the latest offset: ${err}`);
    } finally {
        await saveSession(linesProcessed, options);
        process.exit(0); // Exit the process after handling the shutdown gracefully
    }
}

function createOptions(programOptions) {
    return {
        brokers: [programOptions.brokerUrl],
        clientId: 'kcatpub-producer',
        input: programOptions.file,
        topic: programOptions.topic,
        maxBatch: programOptions.batchSize,
        session: programOptions.session,
    };
}

// Global variables
const sessionFile = 'session.json';
let producer;
let linesProcessed = 0; 
let lastSendPromise = Promise.resolve();
let shutdown = false;
let options = createOptions(parseCommandline());

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);
run(options).catch(error => console.error(`Failed to run: ${error.message}`));

