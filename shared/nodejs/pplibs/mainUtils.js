const crypto = require('crypto')
var CryptoJS = require("crypto-js");

const algorithm = 'aes-256-cbc';

exports.generateToken = (len = 10) => {
    return crypto.randomBytes(Math.ceil(len / 2))
        .toString('hex') // convert to hexadecimal format
        .slice(0, len);   // return required number of characters
}

exports.encrypt = (text, password) => {
    return CryptoJS.AES.encrypt(text, password).toString();
}

// A decrypt function 
exports.decrypt = (text, password) => {
    var bytes  = CryptoJS.AES.decrypt(text, password);
    return bytes.toString(CryptoJS.enc.Utf8)
} 