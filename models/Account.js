const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const RequestSchema = new Schema({
	user_id: { type: String, index: { unique: true }},
	nama: String,
	nilai_saldo: Number
});

module.exports = mongoose.model('Account', RequestSchema);
