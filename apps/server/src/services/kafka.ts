import {Kafka, Producer} from 'kafkajs'
import fs from 'fs'
import path from 'path'
import prismaClient from "./prisma";

const kafka=new Kafka({
    brokers:['kafka-1d7b9935-anandakbari9-350e.a.aivencloud.com:12048'],
    ssl:{
        ca:[fs.readFileSync(path.resolve("./ca (1).pem"),'utf-8')]
    },
    sasl:{
        username:"avnadmin",
        password:"AVNS_onWruhjigY_ghpKhntA",
        mechanism:'plain',
    },
});

let producer:null|Producer=null
export async function createProducer(){
    const _producer=kafka.producer()
    await _producer.connect();
    producer=_producer;
    return producer;
}

export async function producerMessage(message:string){
    const producer=await createProducer();
    await producer.send({
        messages:[{key:`message-${Date.now()}`,value:message}],
        topic:'MESSAGES',
    });
    return true;
}

export async function startMessageConsumer() {
    console.log("Consumer is running..");
    const consumer = kafka.consumer({ groupId: "default" });
    await consumer.connect();
    await consumer.subscribe({ topic: "MESSAGES", fromBeginning: true });
  
    await consumer.run({
      autoCommit: true,
      eachMessage: async ({ message, pause }) => {
        if (!message.value) return;
        console.log(`New Message Recv..`);
        try {
          await prismaClient.message.create({
            data: {
              text: message.value?.toString(),
            },
          });
        } catch (err) {
          console.log("Something is wrong");
          pause();
          setTimeout(() => {
            consumer.resume([{ topic: "MESSAGES" }]);
          }, 60 * 1000);
        }
      },
    });
  }
export default kafka;