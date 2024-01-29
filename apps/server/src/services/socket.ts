import { Server } from "socket.io";
import { Redis } from "ioredis";

const pub=new Redis({
    host:'redis-2ed03b2c-anandakbari9-350e.a.aivencloud.com',
    port:12035,
    username:"default",
    password:"AVNS_On71WnulraD3I-gA74x",
    connectTimeout: 10000
});
const sub=new Redis({
    host:'redis-2ed03b2c-anandakbari9-350e.a.aivencloud.com',
    port:12035,
    username:"default",
    password:"AVNS_On71WnulraD3I-gA74x"
});


class SocketService{
    private _io:Server;
    constructor(){
        console.log("init server");
        this._io=new Server({
            cors:{
                allowedHeaders:['*'],
                origin:'*'
            }
        });
        sub.subscribe("MESSAGES");
    }

    public initListeners(){
        console.log("init socket service");
        const io=this.io;
        console.log("Init Socket Listeners...");
        io.on("connect", (socket) => {
            console.log(`New Socket Connected`, socket.id);
            socket.on("event:message", async ({ message }: { message: string }) => {
              console.log("New Message Rec.", message);

              //publish msg to redis
              await pub.publish('MESSAGES',JSON.stringify({message}));
            });
          });

          sub.on('message',(channel,message)=>{
            if(channel==="MESSAGES"){
                io.emit('message',message);
            }
          });
    }

    get io(){
        return this._io;
    }
}

export default SocketService;