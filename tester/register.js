const Bluebird = require('bluebird');
const amqp = require('amqplib');
const mongoose = require('mongoose');
const moment = require('moment');
const _ = require('lodash');
const util = require('util');

const MY_NPM = process.env.NPM || '1306381704'

if(process.argv.length < 5){
  console.log("usage: node register <npm_target> <user_id> <nama>");
  process.exit(0);
}

const exchange = 'EX_REGISTER';
const type = 'direct';
const durable = true;

const requestBody = {
  action: 'register',
  user_id: process.argv[3],
  nama: process.argv[4],
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
      console.log('result: ')
      console.log(msg.content.toString());
      conn.close();
      process.exit(0);
     }, {noAck: true});
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
