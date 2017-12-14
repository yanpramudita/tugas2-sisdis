const Bluebird = require('bluebird');
const amqp = require('amqplib');
const mongoose = require('mongoose');
const moment = require('moment');
const _ = require('lodash');
const util = require('util');

const Account = require('../models/Account');

const mongoURI = process.env.MONGO_URI || 'mongodb://localhost:27017/ewallet-uas';

const MY_NPM = process.env.NPM || '1306381704';

mongoose.Promise = Bluebird;
mongoose.connect(mongoURI, {
  useMongoClient: true,
  autoReconnect:true
});

if(process.argv.length < 5){
  console.log("usage: node transferFromOwnMachine <npm_target> <user_id> <nilai_transfer>");
  process.exit(0);
}

const exchange = 'EX_TRANSFER';
const type = 'direct';
const durable = true;

const requestBody = {
  action: 'transfer',
  user_id: process.argv[3],
  nilai: process.argv[4],
  sender_id: MY_NPM,
  type: 'request',
  ts: moment(new Date()).format('YYYY-MM-DD HH:mm:ss')
}

let ch;
let conn;
amqp.connect('amqp://sisdis:sisdis@172.17.0.3:5672')
  .then((connection) => {
    console.log('CONNECTED!');
    conn = connection;
    return conn.createChannel();
  })
  .then((channel) => {
    ch = channel
    ch.assertExchange(exchange, type, {durable: durable});
    return ch.assertQueue('', {exclusive: true});
  })
  .then((q) => {
    ch.bindQueue(q.queue, exchange, util.format('RESP_%s', MY_NPM));
    return ch.consume(q.queue, function(msg) {
      console.log('result: ');
      console.log(msg.content.toString());
      try{
        const data = JSON.parse(msg.content.toString());
        if(data.status_transfer > 0){
          Account.findOne({user_id: requestBody.user_id})
            .then((account) => {
              account.nilai_saldo = account.nilai_saldo - requestBody.nilai
              return account.save();
            })
            .then(() => {
              conn.close();
              process.exit(0);
            })
            .catch((e) => {
              console.error(e);
              conn.close();
              process.exit(0);
            });
        }else {
          conn.close();
          process.exit(0);
        }
      }catch (e){
        console.error(e);
        conn.close();
        process.exit(0);
      }
     }, {noAck: true});
  })
  .then(() => Account.findOne({user_id: requestBody.user_id}))
  .then((account) => {
    if(_.isUndefined(account) || _.isNull(account)) {
      console.log('akun tidak ada di kantor cabang anda');
      conn.close();
      process.exit(0);
    }
    if(requestBody.nilai > account.nilai_saldo){
      console.log('saldo tidak cukup');
      conn.close();
      process.exit(0);
    }
    return Bluebird.resolve();
  })
  .then(() => {
    console.log('publish request...');
    ch.assertExchange(exchange, type, {durable: durable});
    return ch.publish(exchange, util.format('REQ_%s', process.argv[2]), new Buffer(JSON.stringify(requestBody)))
  })
  .then((res) => console.log('request published! response = ' + res))
  .catch((e) => {
    if(!_.isUndefined(conn)){
      conn.close();
    }
    console.error(e);
    process.exit(1);
  })
