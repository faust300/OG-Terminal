"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Deduplication = void 0;
const moment_1 = __importDefault(require("moment"));
const Logger_1 = require("./utils/Logger");
const index_1 = require("./index");
class DeduplicationManager {
    constructor() {
        this.txHashs = new Map();
        this.cursors = new Map();
    }
    getCache(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const contract = yield index_1.redis.hGetAll(key);
            return contract;
        });
    }
    checkCache(key, cursorIndex) {
        return __awaiter(this, void 0, void 0, function* () {
            const now = new Date().getTime();
            const nowUnixTime = (0, moment_1.default)(now).unix();
            const contract = yield index_1.redis.hGetAll(key);
            const contractBoolean = contract.next;
            const endPointBoolean = !Number(contract.endPoint) || Number(contract.endPoint) === 0 ? false : true;
            let returnBoolean = false;
            if (!endPointBoolean) {
                if (contractBoolean) {
                    if (Number(contract.cursorIndex) < cursorIndex) {
                        index_1.redis.hSet(key, {
                            cursorIndex: cursorIndex
                        });
                    }
                    returnBoolean = true;
                }
                else {
                    index_1.redis.hSet(key, {
                        next: 'undefined',
                        cursorIndex: cursorIndex,
                        timeStamp: String(nowUnixTime),
                        endPoint: 0
                    });
                    returnBoolean = true;
                }
            }
            return returnBoolean;
        });
    }
    deleteCursor(key) {
        return __awaiter(this, void 0, void 0, function* () {
            yield index_1.redis.del(key);
        });
    }
    updateEndPoint(key) {
        return __awaiter(this, void 0, void 0, function* () {
            index_1.redis.hSet(key, {
                endPoint: 1
            });
        });
    }
    getCursor(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const contract = yield index_1.redis.hGetAll(key);
            return Number(contract.cursorIndex);
        });
    }
    upCursor(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const contract = yield index_1.redis.hGetAll(key);
            const value = Number(contract.cursorIndex);
            if (value) {
                index_1.redis.hSet(key, {
                    cursorIndex: value + 1,
                });
            }
        });
    }
    updateNext(key, next) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                index_1.redis.hSet(key, {
                    next: next
                });
            }
            catch (error) {
                Logger_1.logger.info(error);
            }
        });
    }
}
exports.Deduplication = new DeduplicationManager();
