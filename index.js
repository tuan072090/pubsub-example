// Imports the Google Cloud client library
const {PubSub} = require('@google-cloud/pubsub');

// Creates a client
const pubsub = new PubSub();


async function listAllTopics() {

    // Lists all topics in the current project
    const [topics] = await pubsub.getTopics();
    console.log('Topics:');
    topics.forEach(topic => console.log(topic.name));
    // [END pubsub_list_topics]
}

/**
 * TODO: Create a new topic by name
 */
function createTopic(topicName) {
    pubsub
        .createTopic(topicName)
        .then(res => {
            console.log("tạo topic success", res);
        })
        .catch(err => {
            console.error('tạo topic ERROR:', err);
        });
}

/**
 * TODO: Delete a new topic by name
 */
function deleteTopic(topicName) {
    pubsub.topic(topicName).delete()
        .then(res => {
            console.log("delete topic success", res);
        })
        .catch(err => {
            console.error('delete topic ERROR:', err);
        });

}

/**
 * TODO: Create a subscription for a topic
 */
function createSubscription(topicName, subscriptionName){
    pubsub.topic(topicName).createSubscription(subscriptionName)
        .then(res => {
            console.log("delete topic success", res);
        })
        .catch(err => {
            console.error('delete topic ERROR:', err);
        });

}

/**
 * TODO: push message to topic
 */
function pushMessage() {
    const data = new Date().toString();
    const dataBuffer = Buffer.from(data);

    const topicName = "my-topic";

    pubsub
        .topic(topicName)
        .publish(dataBuffer, {
            name: "Lợi",
            lastName: "Dương",
            Foo: "BAR"
        })
        .then(messageId => {
            console.log(`Message ${messageId} published.`);
        })
        .catch(err => {
            console.error('PubERROR:', err);
        });
}

/**
 * TODO: list all subscription of a topic
 */
async function listTopicSubscriptions(topicName) {

    // Lists all subscriptions for the topic
    const [subscriptions] = await pubsub.topic(topicName).getSubscriptions();

    subscriptions.forEach(subscription => {
        console.log("list subscription of topic " + topicName);
        console.log("subscription: ", subscription.name)
    });
}


/**
 * TODO: References an existing subscription
 */
function pullMessage(subscriptionName) {
    const subscription = pubsub.subscription(subscriptionName);

    // Create an event handler to handle messages
    let messageCount = 0;
    const messageHandler = message => {
        console.log("Received message....... :", message.id);
        console.log("Data......:",message.data);
        console.log("Attributes.........:", message.attributes);

        messageCount += 1;

        // "Ack" (acknowledge receipt of) the message
        message.ack();
    };

    // Listen for new messages until timeout is hit
    subscription.on(`message`, messageHandler);

    setTimeout(() => {
        subscription.removeListener('message', messageHandler);
        console.log(`${messageCount} message(s) received.`);
    }, 600000);
}

console.log("GOOGLE_APPLICATION_CREDENTIALS......", process.env['GOOGLE_APPLICATION_CREDENTIALS'] );

// listAllTopics();

// pullMessage("thong-sub");
