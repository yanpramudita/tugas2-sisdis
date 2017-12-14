const Bluebird = require('bluebird');
const amqp = require('amqplib');
const mongoose = require('mongoose');
const moment = require('moment');
const _ = require('lodash');
const util = require('util');

const Pinger = require('./models/Pinger');
const Account = require('./models/Account');

const mongoURI = process.env.MONGO_URI || 'mongodb://localhost:27017/ewallet-uas';
const MY_NPM = process.env.NPM || '1306381704'

var conn;

mongoose.Promise = Bluebird;
mongoose.connect(mongoURI, {
  useMongoClient: true,
  autoReconnect:true
});

const GET_TOTAL_SALDO_TARGET = [
  '1406579100',  // wahyu
  '1406527513', //zamil
  '1306381704'  //ian
]

const QUORUM_SIZE = 8;

var PING_PUBLISHER_CHANNEL;
var GET_SALDO_PUBLISHER_CHANNEL;
var REGISTER_PUBLISHER_CHANNEL;
var TRANSFER_PUBLISHER_CHANNEL;
var GET_TOTAL_SALDO_PUBLISHER_CHANNEL;

const PING_EXCHANGE = 'EX_PING';
const REGISTER_EXCHANGE = 'EX_REGISTER';
const GET_SALDO_EXCHANGE = 'EX_GET_SALDO';
const TRANSFER_EXCHANGE = 'EX_TRANSFER';
const GET_TOTAL_SALDO_EXCHANGE = 'EX_GET_TOTAL_SALDO';

function populatePublisher(){
  Bluebird.all([
    createPublisherChannel(PING_EXCHANGE, 'fanout', false),
    createPublisherChannel(GET_SALDO_EXCHANGE, 'direct', true),
    createPublisherChannel(REGISTER_EXCHANGE, 'direct', true),
    createPublisherChannel(TRANSFER_EXCHANGE, 'direct', true),
    createPublisherChannel(GET_TOTAL_SALDO_EXCHANGE, 'direct', true)
  ]).spread((channel1, channel2, channel3, channel4, channel5) => {
    PING_PUBLISHER_CHANNEL = channel1;
    GET_SALDO_PUBLISHER_CHANNEL = channel2;
    REGISTER_PUBLISHER_CHANNEL = channel3;
    TRANSFER_PUBLISHER_CHANNEL = channel4;
    GET_TOTAL_SALDO_PUBLISHER_CHANNEL = channel5;
    return Bluebird.resolve();
  }).catch((e) => {
    return Bluebird.reject(e);
  });
}

function createPublisherChannel(exchange, type, durable){
  return new Bluebird((resolve, reject) => {
    return conn.createChannel()
      .then((ch) => {
        ch.assertExchange(exchange, type, {durable: durable});
        return resolve(ch);
      })
      .catch((e) => {
        return reject(e);
      });
    });
}

function consume(exchange, type, handler, durable, routingKey){
  return conn.createChannel()
    .then((ch) => {
      ch.assertExchange(exchange, type, {durable: durable});
      return Bluebird.resolve(ch);
    })
    .then((ch) => {
      return ch.assertQueue('', {exclusive: true})
        .then((q) => Bluebird.all([
          Bluebird.resolve(q),
          Bluebird.resolve(ch)
        ]));
    })
    .spread((q,ch) => {
      ch.bindQueue(q.queue, exchange, routingKey);
      return ch.consume(q.queue, function(msg) {
        handler(msg);
       }, {noAck: true});
    })
    .catch(console.warn);
}

function handlePing(msg){
  try{
    const pinger = JSON.parse(msg.content.toString());
    Pinger.update({npm: pinger.npm}, pinger, {upsert: true, setDefaultsOnInsert: true})
    .catch((e) => console.error((e)));
  }catch (e){
    console.error(e);
  }
}

function ping(){
  const message = {
    action: 'ping',
    npm: MY_NPM,
    ts: moment(new Date()).format('YYYY-MM-DD HH:mm:ss')
  };
  return PING_PUBLISHER_CHANNEL.publish(PING_EXCHANGE, '', new Buffer(JSON.stringify(message)));
}

function handleRegister(msg){
  try{
    var response = {
      action: 'register',
      type: 'response'
    }
    const data = JSON.parse(msg.content.toString());
    getQuorum().then((quorum) => {
      if (quorum < QUORUM_SIZE/2) {
        console.error(util.format('quorum tidak memenuhi 50%, hanya %s/%s', (quorum, QUORUM_SIZE)));
        return Bluebird.reject(-2);
      }
      return Bluebird.resolve();
    })
    .then(() => register(data.user_id, data.nama))
    .then((status_register) => {
      response.status_register = status_register;
      return Bluebird.resolve();
    })
    .catch(errorStatus => {
      if(!_.isNumber(errorStatus)){
        console.error(errorStatus)
      }
      errorStatus = _.isNumber(errorStatus) ? errorStatus : -99;
      response.status_register = errorStatus;
    })
    .finally(() => {
      response.ts = moment(new Date()).format('YYYY-MM-DD HH:mm:ss');
      return REGISTER_PUBLISHER_CHANNEL.publish(
        REGISTER_EXCHANGE,
        util.format('RESP_%s', data.sender_id),
        new Buffer(JSON.stringify(response))
      );
    });
  }catch (e){
    console.error(e);
  }
}

function handleGetSaldo(msg){
  try{
    var response = {
      action: 'get_saldo',
      type: 'response'
    }
    const data = JSON.parse(msg.content.toString());
    getQuorum().then((quorum) => {
      if (quorum < QUORUM_SIZE/2) {
        console.error(util.format('quorum tidak memenuhi 50%, hanya %s/%s', (quorum, QUORUM_SIZE)));
        return Bluebird.reject(-2);
      }
      return Bluebird.resolve();
    })
    .then(() => getSaldo(data.user_id))
    .then((nilai_saldo) => {
      response.nilai_saldo = nilai_saldo;
      return Bluebird.resolve();
    })
    .catch(errorStatus => {
      if(!_.isNumber(errorStatus)){
        console.error(errorStatus)
      }
      errorStatus = _.isNumber(errorStatus) ? errorStatus : -99;
      response.nilai_saldo = errorStatus;
    })
    .finally(() => {
      response.ts = moment(new Date()).format('YYYY-MM-DD HH:mm:ss')
      return GET_SALDO_PUBLISHER_CHANNEL.publish(
        GET_SALDO_EXCHANGE,
        util.format('RESP_%s', data.sender_id),
        new Buffer(JSON.stringify(response))
      );
    });
  }catch (e){
    console.error(e);
  }
}

function handleTransfer(msg){
  try{
    var response = {
      action: 'transfer',
      type: 'response'
    }
    const data = JSON.parse(msg.content.toString());
    getQuorum().then((quorum) => {
      if (quorum < QUORUM_SIZE/2) {
        console.error(util.format('quorum tidak memenuhi 50%, hanya %s/%s', (quorum, QUORUM_SIZE)));
        return Bluebird.reject(-2);
      }
      return Bluebird.resolve();
    })
    .then(() => transfer(data.user_id, data.nilai))
    .then((status_transfer) => {
      response.status_transfer = status_transfer;
      return Bluebird.resolve();
    })
    .catch(errorStatus => {
      if(!_.isNumber(errorStatus)){
        console.error(errorStatus)
      }
      errorStatus = _.isNumber(errorStatus) ? errorStatus : -99;
      response.status_transfer = errorStatus;
    })
    .finally(() => {
      response.ts = moment(new Date()).format('YYYY-MM-DD HH:mm:ss')
      return TRANSFER_PUBLISHER_CHANNEL.publish(
        TRANSFER_EXCHANGE,
        util.format('RESP_%s', data.sender_id),
        new Buffer(JSON.stringify(response))
      );
    });
  }catch (e){
    console.error(e);
  }
}

function handleGetTotalSaldo(msg){
  try{
    var response = {
      action: 'get_total_saldo',
      type: 'response',
      sender_id: MY_NPM
    }
    const data = JSON.parse(msg.content.toString());
    getQuorum().then((quorum) => {
      if (quorum < QUORUM_SIZE/2) {
        console.error(util.format('quorum tidak memenuhi 50%, hanya %s/%s', (quorum, QUORUM_SIZE)));
        return Bluebird.reject(-2);
      }
      return Bluebird.resolve();
    })
    .then(() => getTotalSaldo(data.user_id))
    .then((nilai_saldo) => {
      response.nilai_saldo = nilai_saldo;
      return Bluebird.resolve();
    })
    .catch(errorStatus => {
      if(!_.isNumber(errorStatus)){
        console.error(errorStatus)
      }
      errorStatus = _.isNumber(errorStatus) ? errorStatus : -99;
      response.nilai_saldo = errorStatus;
    })
    .finally(() => {
      response.ts = moment(new Date()).format('YYYY-MM-DD HH:mm:ss')
      return GET_TOTAL_SALDO_PUBLISHER_CHANNEL.publish(
        GET_TOTAL_SALDO_EXCHANGE,
        util.format('RESP_%s', data.sender_id),
        new Buffer(JSON.stringify(response))
      );
    });
  }catch (e){
    console.error(e);
  }
}

function callOtherTotalSaldo(user_id, target){
  const message = {
    action: 'get_saldo',
    user_id: user_id,
    sender_id: MY_NPM,
    type: 'request',
    ts: moment(new Date()).format('YYYY-MM-DD HH:mm:ss')
  };
  return new Bluebird((resolve, reject) => {
    let channel;
    return Bluebird.resolve()
    .then(() => {
      return GET_SALDO_PUBLISHER_CHANNEL.publish(
        GET_SALDO_EXCHANGE,
        util.format('REQ_%s', target),
        new Buffer(JSON.stringify(message))
      );
    })
    .catch(e => {
      if(!_.isUndefined(channel)){
        channel.close();
      }
      return reject(e);
    });
  });
}

function getTotalSaldo(user_id) {
  if(_.isUndefined(user_id)) {
    return Bluebird.reject(-99);
  }
  return new Bluebird((resolve, reject) => {
      let count = 0;
      let totalSaldo = 0;
      let channel;
      return Bluebird.resolve()
      .then(() => {
        setTimeout(() => {
          if(!_.isUndefined(channel)){
            channel.close();
          }
          reject(-99)
        }, 10000);
        return conn.createChannel()
          .then((ch) => {
            channel = ch;
            ch.assertExchange(GET_SALDO_EXCHANGE, 'direct', {durable: true});
            return Bluebird.resolve(ch);
          })
          .then((ch) => {
            channel = ch;
            return ch.assertQueue('', {exclusive: true})
              .then((q) => Bluebird.all([
                Bluebird.resolve(q),
                Bluebird.resolve(ch)
              ]));
          })
          .spread((q,ch) => {
            ch.bindQueue(q.queue, GET_SALDO_EXCHANGE, util.format('RESP_%s', MY_NPM));
              ch.consume(q.queue, function(msg) {
                try{
                  const data = JSON.parse(msg.content.toString());
                  if(data.nilai_saldo < 0){
                    return reject(data.nilai_saldo);
                  }
                  totalSaldo += data.nilai_saldo;
                  if(++count >= GET_TOTAL_SALDO_TARGET.length){
                    if(!_.isUndefined(channel)){
                      channel.close();
                    }
                    return resolve(totalSaldo);
                  }
                }catch (e){
                  if(!_.isUndefined(channel)){
                    channel.close();
                  }
                  return reject(e);
                }
               }, {noAck: true});
          })
          .catch(e => Bluebird.reject(e))
      })
      .then(() => Bluebird.map(GET_TOTAL_SALDO_TARGET, function(target) {
          return callOtherTotalSaldo(user_id, target);
      }))
      .catch(err => reject(err));
  });
}

function getSaldo(user_id) {
  if(_.isUndefined(user_id)) {
    return Bluebird.reject(-99);
  }
  if(mongoose.connection.readyState != 1){
    return Bluebird.reject(-4);
  }
  return new Bluebird((resolve, reject) => {
    Account.findOne({user_id: user_id})
      .then((account) => {
        if(_.isUndefined(account) || _.isNull(account)) {
          return reject(-1);
        }
        return resolve(account.nilai_saldo);
      })
      .catch(err => {
        return reject(-4);
      });
  });
}

function transfer(user_id, value) {
  if(_.isUndefined(user_id)) {
    return Bluebird.reject(-99);
  }
  if(_.isUndefined(value) || !_.isInteger(_.parseInt(value))
    || _.parseInt(value) < 0 || _.parseInt(value) > 1000000000
  ) {
    return Bluebird.reject(-5);
  }
  if(mongoose.connection.readyState != 1){
    return Bluebird.reject(-4);
  }
  return new Bluebird((resolve, reject) => {
    Account.findOne({user_id: user_id})
      .then((account) => {
        if(_.isUndefined(account) || _.isNull(account)) {
          return reject(-1);
        }
        account.nilai_saldo += _.parseInt(value);
        account.save();
        return resolve(1);
      })
      .catch(err => {
        return reject(-4);
      });
  });
}

function register(user_id, nama) {
  if(_.isUndefined(user_id) || _.isUndefined(nama)) {
    return Bluebird.reject(-99);
  }
  if(mongoose.connection.readyState != 1){
    return Bluebird.reject(-4);
  }
  return new Bluebird((resolve, reject) => {
    Account.findOne({user_id: user_id})
      .then((account) => {
        if(!_.isUndefined(account) && !_.isNull(account)) {
          return reject(-4);
        }
        account = new Account();
        account.user_id = user_id;
        account.nama = nama;
        account.nilai_saldo = 0;
        account.save();
        return resolve(1);
      })
      .catch(err => {
        return reject(-4);
      });
  });
}

function getQuorum() {
  return new Bluebird((resolve, reject) => {
    const time = new Date();
    time.setSeconds(time.getSeconds()-10);
    Pinger
    .find({ts : { $gte : time} })
    .limit(QUORUM_SIZE)
    .exec()
    .then((pingers) => resolve(pingers.length))
    .catch((err) => {
      console.warn;
      return reject(err);
    });
  });
}

amqp.connect('amqp://sisdis:sisdis@172.17.0.3:5672')
  .then((connection) => {
    console.log("CONNECTED on amqp server")
    conn = connection;
    return Bluebird.resolve();
  }).then(() => {
    setInterval(ping, 5000);
    return Bluebird.all([
      consume(PING_EXCHANGE, 'fanout', handlePing, false, ''),
      consume(REGISTER_EXCHANGE, 'direct', handleRegister, true, util.format('REQ_%s', MY_NPM)),
      consume(GET_SALDO_EXCHANGE, 'direct', handleGetSaldo, true, util.format('REQ_%s', MY_NPM)),
      consume(GET_TOTAL_SALDO_EXCHANGE, 'direct', handleGetTotalSaldo, true, util.format('REQ_%s', MY_NPM)),
      consume(TRANSFER_EXCHANGE, 'direct', handleTransfer, true, util.format('REQ_%s', MY_NPM)),
      populatePublisher()
    ]);
  })
  .then(() => setInterval(ping, 5000))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
