"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getNameABI = exports.httpRequestAssets = exports.httpRequestCollection = void 0;
const axios_1 = __importDefault(require("axios"));
const https_1 = __importDefault(require("https"));
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
exports.httpRequestCollection = axios_1.default.create({
    timeout: 60000,
    httpsAgent: new https_1.default.Agent({ keepAlive: true }),
    headers: { 'Content-Type': 'application/json', 'X-API-KEY': process.env.OPENSEA_API_KEY }
});
exports.httpRequestAssets = axios_1.default.create({
    timeout: 60000,
    httpsAgent: new https_1.default.Agent({ keepAlive: true }),
    headers: { 'Content-Type': 'application/json', 'X-API-KEY': process.env.OPENSEA_API_KEY2 }
});
const getNameABI = () => {
    return [
        {
            "inputs": [],
            "name": "name",
            "outputs": [
                {
                    "internalType": "string",
                    "name": "",
                    "type": "string"
                }
            ],
            "stateMutability": "view",
            "type": "function"
        }
    ];
};
exports.getNameABI = getNameABI;
