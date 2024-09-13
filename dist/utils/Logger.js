"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.logger = void 0;
const winston_1 = __importDefault(require("winston"));
const { combine, timestamp, printf } = winston_1.default.format;
const logFormat = printf(info => {
    return `[${info.timestamp}] [${info.level}] : ${info.message}`;
});
exports.logger = winston_1.default.createLogger({
    format: combine(winston_1.default.format.colorize(), timestamp({
        format: 'YYYY-MM-DD HH:mm:ss',
    }), logFormat),
    transports: []
}).add(new winston_1.default.transports.Console({
    format: winston_1.default.format.combine(winston_1.default.format.colorize(), timestamp({
        format: 'YYYY-MM-DD HH:mm:ss',
    }), logFormat)
}));
