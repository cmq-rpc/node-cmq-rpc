module.exports = {
    RPCRemoteException: class RPCRemoteException extends Error{
        constructor(message, code){
            super(message);
            this.code = code
        }

        getMessage(){
            return this.message;
        }
        getCode(){
            return this.code;
        }
    }
}