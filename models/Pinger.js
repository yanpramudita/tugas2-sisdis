const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const RequestSchema = new Schema({
	npm: { type: String, index: { unique: true }},
	ts: { type: Date, index: true }
});

module.exports = mongoose.model('Pinger', RequestSchema);
