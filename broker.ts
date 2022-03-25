const amqp = require("amqplib/callback_api");

import ResponseModel from "./ResponseModel";
import Exchange from "./exchange";

interface QueueUrlMapValue {
  queueName: string;
  OnSuccessTopicToPush: string[];
  OnFailureTopicToPush: string[];
}

export class Broker {
  private static instace: Broker;
  private queueUrlMap: { [id: string]: QueueUrlMapValue } = {}; //think if it like dictionary with queueName as the key
  private channel = null;
  private rabbitmqUrl: string = `amqp://127.0.0.1:5672`;
  private topicNames: string[] = [];

  private constructor() {
    this.init_Broker();
  }

  private static getInstance(): Broker {
    if (!Broker.instace) {
      Broker.instace = new Broker();
    }
    return Broker.instace;
  }

  // initiate all the exchanges
  private async init_Broker() {
    try {
      console.log(" Connecting to RabbitMQ . . .", this.rabbitmqUrl);
      amqp.connect(this.rabbitmqUrl, (err, connection) => {
        // A Connection is created by opening a physical TCP connection to the target server
        connection.createChannel((err, channel) => {
          // Connection is a link between client and the broker
          // A connection can multiplex into several 'light-weight connections' called Channel.
          // Each Connection can maintain a set of underlying Channels.
          // Instead of having many heavy-weight connections, a service can reuse the resources by creating and deleting Channels.

          this.channel = channel;
          const topics = Exchange.Topics;

          for (let i = 0; i < topics.length; i++) {
            let topic = topics[i];
            let topicName = topic.TopicName;
            this.topicNames.push(topicName);

            // the exchange is the first entry point for a message entering the message broker.
            // exchange is responsible for routing messages to different queues.

            this.channel.assertExchange(topicName, "fanout", { durable: true });
            //fanout exchange copies and routes a receive message to all queues that are bound to it.
            // Exchanges, just like queues, can be configured with parameters such as durable, temporary, and auto-delete upon creation.
            // durable exchanges survive server restart
            // durable queue specifies if it will survive a broker restart

            let subscribers = topic.Subscribers;
            for (let j = 0; j < subscribers.length; j++) {
              let subscriber = subscribers[j];
              let queueName = subscriber.QueueName;
              // a queue always has a name, if not declared most client lib use a random genrated name

              this.channel.assertQueue(queueName, {
                // A queue can be exclusive, which specifies if the queue can be used by only one connection.
                // An exclusive queue is deleted when that connection closes.
                exclusive: false,
              });

              //binding
              this.channel.bindQueue(queueName, topicName, ""); // " " is just the routing key , it will get ignore in fanout exchange
              let QueueUrlMapValue = {
                queueName: queueName,
                OnSuccessTopicToPush: subscriber.OnSuccessTopicsToPush,
                OnFailureTopicToPush: subscriber.OnFailureTopicsToPush,
              };

              this.queueUrlMap[queueName] = QueueUrlMapValue;
            }
          }
        });
      });
    } catch (err) {
      console.log(err.message, "Check If your rabbitMq is running");
    }
  }

  public publishMessageToTopic(
    topicName: string,
    message: any
  ): ResponseModel<{}> {
    console.log("Message is received in the broker to publish : ", message);

    // Before pulishing into topic, we need to make stringified message to buffer
    const data = Buffer.from(JSON.stringify(message));

    //Publish message to topic
    let response: ResponseModel<{}>;

    if (this.topicNames.includes(topicName)) {
      this.channel.publish(topicName, "", data);
      // "" is the routingKey, is of no use here because of fanout exchange
      response = new ResponseModel(
        200,
        "SUCCESS",
        "POST",
        `Successfully pulished the message into Topic Name: ${topicName}`,
        {}
      );
    } else {
      response = new ResponseModel(
        400,
        "FAILED",
        "POST",
        `Unable to publish the message into Topic Name: ${topicName}`,
        {}
      );
    }

    return response;
  } // Once an acknowledgment is received, the message can be discarded from the queue. I

  // Listen to a particular queue
  public async listenToService(topicName, serviceName, callback) {
    try {
      const QueueUrlMapValue = this.queueUrlMap[topicName + "-" + serviceName];
      const queueName = QueueUrlMapValue.queueName;
      //consume message from queue
      this.channel.consume(
        queueName,
        (msg) => {
          if (msg.content) {
            let message = JSON.parse(msg.content);
            callback({
              message,
              OnSuccessTopicToPush: QueueUrlMapValue.OnSuccessTopicToPush,
              OnFailureTopicToPush: QueueUrlMapValue.OnFailureTopicToPush,
            });
          }
        },
        //Acknowledgement
        { noAck: true }
      );
    } catch (err) {
      setTimeout(() => {
        this.listenToService(topicName, serviceName, callback);
      }, 5000);
    }
  }

  public listenToServices(serviceName, callback) {
    let topics = Exchange.Topics;
    for (let i = 0; i < topics.length; i++) {
      let topic = topics[i];
      let topicName = topic.TopicName;
      let subscribers = topic.Subscribers;
      for (let j = 0; j < subscribers.length; j++) {
        let subscriber = subscribers[j];
        let service_Name = subscriber.Service;
        if (service_Name === serviceName) {
          this.listenToService(topicName, serviceName, callback);
        }
      }
    }
  }
  //A callback is a function passed as an argument to another function
  //This technique allows a function to call another function
  //A callback function can run after another function has finished

  // AMQP commands such as "queue.create" and "exchange.create" are all sent over a channel.
  // Closing a connection closes all associated channels.
}
