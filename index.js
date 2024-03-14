const fs = require('fs');
const { Kafka, Partitioners } = require('kafkajs');
const readline = require('readline');
const { program } = require('commander');

class SessionManager {
    constructor(sessionFile = 'session.json') {
        this.sessionFile = sessionFile;
    }

    async saveSession(offset, options) {
        await fs.promises.writeFile(this.sessionFile, JSON.stringify({ startIndex: offset, options }), { encoding: 'utf8' });
        console.log(`Successfully saved the latest offset (${offset}) to session. Use -s option to continue from where it left off.`);
    }

    async getSessionOffset() {
        try {
            if (fs.existsSync(this.sessionFile)) {
                const fileContent = await fs.promises.readFile(this.sessionFile, { encoding: 'utf8' });
                const sessionSettings = JSON.parse(fileContent);
                return isNaN(sessionSettings.startIndex) ? 0 : sessionSettings.startIndex;
            }
        } catch (err) {
            console.error(`Error reading session file: ${err}`);
            throw err;
        }
        return 0;
    }
}

class KafkaPublisher {
    constructor(options, sessionManager) {
        this.options = options;
        this.sessionManager = sessionManager;
        this.kafka = new Kafka({
            clientId: options.clientId,
            brokers: options.brokers,
        });
        this.producer = this.kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
        this.linesProcessed = 0;
        this.lastSendPromise = Promise.resolve();
    }

    async connect() {
        await this.producer.connect();
    }

    async sendMessagesAsync(messages) {
        try {
            console.log(`Sending message batch of size ${messages.length}.`);
            await this.producer.send({ topic: this.options.topic, messages });
            console.log(`Sent message batch of size ${messages.length}.`);
            this.linesProcessed += messages.length;
        } catch (error) {
            await this.sessionManager.saveSession(this.linesProcessed, this.options);
            throw error;
        }
    }

    async run() {
        const sessionOffset = this.options.session ? await this.sessionManager.getSessionOffset() : 0;
        const rl = readline.createInterface({ input: fs.createReadStream(this.options.input) });
        let batch = [];

        for await (const line of rl) {
            if (global.shutdown) {
                break;
            }

            if (this.linesProcessed < sessionOffset) {
                this.linesProcessed++;
                continue;
            }
            const event = JSON.parse(line);
            const headers = event.headers?.reduce((acc, curr, idx, src) => {
                if (idx % 2 === 0) acc[curr] = src[idx + 1];
                return acc;
            }, {});

            batch.push({
                headers,
                value: event.payload == null ? null : JSON.stringify(event.payload)
            });

            if (batch.length >= this.options.maxBatch) {
                this.lastSendPromise = this.sendMessagesAsync(batch);
                await this.lastSendPromise;
                batch = [];
            }
        }

        if (batch.length > 0) {
            await this.sendMessagesAsync(batch);
        }

        await this.producer.disconnect();
        console.log(`Done with publishing. Exiting.`);
    }
}

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

async function gracefulShutdown(publisher) {
    console.log('Graceful shutdown initiated.');
    global.shutdown = true;
    await publisher.lastSendPromise;
    await publisher.producer.disconnect();
    await publisher.sessionManager.saveSession(publisher.linesProcessed, publisher.options);
    process.exit(0);
}

function createOptions(programOptions) {
    return {
        brokers: [programOptions.brokerUrl],
        clientId: 'kcatpub-producer',
        input: programOptions.file,
        topic: programOptions.topic,
        maxBatch: programOptions.batchSize
        , session: programOptions.session,
    };
}

async function main() {
    const programOptions = parseCommandline();
    const options = createOptions(programOptions);
    const sessionManager = new SessionManager();
    const publisher = new KafkaPublisher(options, sessionManager);

    global.shutdown = false;
    process.on('SIGINT', () => gracefulShutdown(publisher));
    process.on('SIGTERM', () => gracefulShutdown(publisher));

    await publisher.connect();
    await publisher.run().catch(async error => {
        console.error(`Failed to run: ${error.message}`);
        await gracefulShutdown(publisher);
    });
}

main().catch(error => console.error(`Execution failed: ${error.message}`));