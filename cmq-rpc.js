let {CMQ} = require('cmq-sdk'),
    Queue = require('better-queue'),
    debug = require('debug')('CMQ-RPC'),
    events = require('events'),
    util = require("util"),
    once = require('once'),
    RPCRemoteException = require('./exceptions/RPCRemoteException').RPCRemoteException,
    UUIDGen = require('./helpers/UUIDGen');

class CMQRpc{
    constructor(CMQOptions, methods, options){
        this.options = options || {};
        this.CMQOptions = CMQOptions || {};
        this.methods = methods || {};

        this.cmq = CMQ.NEW(this.CMQOptions);
        this._CallRec = {};
        this.CallbackEmitter = new events.EventEmitter();

        this.TaskQueue = new Queue(function (task, cb) {
            return task(cb);
        }, {
            maxRetries: Infinity,
            retryDelay: 3000,
            concurrent: 3,
        });

        setInterval(() => {
            debug.extend('RequestQueueStats')(this.TaskQueue.getStats());
        }, 15000);

        setInterval(() => {
            debug.extend('RequestQueueStats')(this.TaskQueue.resetStats());
        }, 60 * 60 * 1000);

        let job = async (cb) => {
            try{
                let result = await this.cmq.receiveMessage({
                    'queueName': this.CMQOptions.ReceiveQueueName,
                    pollingWaitSeconds: 30,
                });


                debug.extend("ReceivedMessage")(result);
                if(!result.code){
                    await this.handleMessage(result);
                }else if(result.message.indexOf('no message') == -1){
                    /*
                        Ignore for no message

                        debug.extend('QueueError')(result.message);
                        debug.extend('QueueErrorDetail')(result);
                    */
                }

                this.TaskQueue.push((cb_inner) => {
                    return job(cb_inner);
                });

                return cb(null);

            }catch (err) {
                if(err.message.indexOf('no message') == -1){
                    debug.extend('QueueError')(err.message);
                    debug.extend('QueueErrorDetail')(err);
                }
                this.TaskQueue.push((cb_inner) => {
                    return job(cb_inner);
                });

                return cb(null);
            }
        };


        this.TaskQueue.push((cb) => {
            return job(cb);
        })

        events.EventEmitter.call(this);
    }

    __call(method) {
        return ((callback, ...args) => {
            debug.extend("LocalRequest")(callback, args);

            this._method = method;
            let request_id = UUIDGen();

            let Msg = this.packMsg('call', method, args, request_id)

            debug.extend('PackedRequest')(Msg);

            this.cmq.sendMessage({
                queueName: this.CMQOptions.SendQueueName,
                msgBody: Msg,
            });

            this.CallbackEmitter.addListener('callback_' + request_id, (result) => {
                callback = once(callback);
                if(result['exception'] && result['exception']['message']){
                    callback(new RPCRemoteException(result['exception']['message'], result['exception']['code']));
                }else {
                    let params = result['result'];
                    callback(null, params);
                }
                return this.CallbackEmitter.removeAllListeners('callback_' + request_id)
            })
        })
    }

    packMsg(action, method, args, request_id, exception){
        return JSON.stringify({
            action,
            method,
            params: args,
            request_id,
            exception
        })
    }

    handleMessage(Message){
        return new Promise(async (resolve, reject) => {
            try{
                let QueryInfo;
                try{
                    QueryInfo = JSON.parse(Message.msgBody);
                }catch (e) {
                    debug.extend('QueueError')("RPC Body解析错误");
                    debug.extend('QueueErrorDetail')(e, Message);

                    await this.cmq.deleteMessage({
                        queueName: this.CMQOptions.ReceiveQueueName,
                        receiptHandle: Message.receiptHandle
                    })

                    return reject(e)
                }
                debug.extend('HandledMessage')(QueryInfo);

                if(QueryInfo['action'] == "callback"){
                    this.CallbackEmitter.emit(QueryInfo['action'] + "_" + QueryInfo['request_id'], {
                        result: QueryInfo['params'],
                        exception: QueryInfo['exception']
                    })
                    this.cmq.deleteMessage({
                        queueName: this.CMQOptions.ReceiveQueueName,
                        receiptHandle: Message.receiptHandle
                    })
                }else if(QueryInfo['action'] == "call"){
                    if(this.methods[QueryInfo.method] && typeof this.methods[QueryInfo.method] == "function"){
                        //调用注册了的服务
                        this.methods[QueryInfo.method](...QueryInfo['params'], (err, result) => {
                            debug.extend("LocalCallback")(err, result)
                            let Msg;
                            if(err){
                                Msg = this.packMsg('callback', QueryInfo.method, result, QueryInfo.request_id, {
                                    'message': err.message,
                                    'code': err.code
                                })
                            }else{
                                Msg = this.packMsg('callback', QueryInfo.method, result, QueryInfo.request_id)
                            }

                            debug.extend("PackedCallback")(Msg)
                            this.cmq.sendMessage({
                                queueName: this.CMQOptions.SendQueueName,
                                msgBody: Msg,
                            });
                            this.cmq.deleteMessage({
                                queueName: this.CMQOptions.ReceiveQueueName,
                                receiptHandle: Message.receiptHandle
                            })
                        })
                    }
                }

                return resolve()
            }catch (e) {
                debug.extend('QueueError')(e.message);
                debug.extend('QueueErrorDetail')(e);
                return reject(e)
            }
        });
    }
}

util.inherits(CMQRpc, events.EventEmitter);

module.exports = function(CMQOptions, methods, options){
    this._CMQRpc = new CMQRpc(CMQOptions, methods, options);
    return new Proxy(this._CMQRpc,{
            get: function(target, key, receiver) {
                if(target[key] !== undefined){
                    return target[key];
                }else{
                    return target.__call(key);
                }

            }
        }
    );
};
