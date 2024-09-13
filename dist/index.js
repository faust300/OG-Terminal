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
exports.redis = void 0;
const amqmodule_1 = require("amqmodule");
const awsprovider_1 = require("awsprovider");
const dotenv_1 = __importDefault(require("dotenv"));
const libs_connection_pool_1 = __importDefault(require("libs-connection-pool"));
const libs_job_manager_1 = require("libs-job-manager");
const web3_1 = __importDefault(require("web3"));
const CollectJobManager_1 = require("./CollectJobManager");
const DeduplicationManager_1 = require("./DeduplicationManager");
const GetAssetsJob_1 = __importDefault(require("./jobs/opensea/GetAssetsJob"));
const GetCollectionJob_1 = __importDefault(require("./jobs/opensea/GetCollectionJob"));
const CalcRarityJob_1 = __importDefault(require("./jobs/rarity/CalcRarityJob"));
const Logger_1 = require("./utils/Logger");
const redis_1 = require("redis");
dotenv_1.default.config();
const redisURL = `redis://@${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`;
exports.redis = (0, redis_1.createClient)({ url: redisURL });
(() => __awaiter(void 0, void 0, void 0, function* () {
    try {
        yield exports.redis.connect();
    }
    catch (error) {
        Logger_1.logger.info(error);
    }
    const mqInstance = yield (0, amqmodule_1.createJsonTypeInstance)({
        host: String(process.env.MQ_HOST),
        id: String(process.env.MQ_ID),
        pw: String(process.env.MQ_PW),
        port: parseInt(String(process.env.MQ_PORT))
    });
    mqInstance.setExchange(String(process.env.MQ_EXCHANGE));
    const cp = new libs_connection_pool_1.default({
        host: String(process.env.MYSQL_HOST),
        writerHost: String(process.env.MYSQL_HOST),
        readerHost: String(process.env.MYSQL_RO_HOST),
        user: String(process.env.MYSQL_USER),
        password: String(process.env.MYSQL_PASSWORD),
        database: String(process.env.MYSQL_DATABASE),
    });
    const web3 = new web3_1.default(awsprovider_1.Provider.from({
        type: awsprovider_1.ProviderTypes.HTTP,
        endpoint: String(process.env.HTTPS_END_POINT),
        accessKeyId: String(process.env.ACCESS_KEY_ID),
        secretAccessKey: String(process.env.SECRET_ACCESS_KEY),
    }));
    Logger_1.logger.info("[Terminal Asset Recoder] [Ready for Message Queue]");
    const jobManager = new CollectJobManager_1.CollectJobManager(cp, mqInstance).setMode(libs_job_manager_1.Mode.Async);
    mqInstance.consume(String(process.env.MQ_QUEUE_COLLECTION), (payload, done) => {
        Logger_1.logger.info("[Terminal Asset Recoder] [Consume GetCollectionJob]");
        jobManager.addJob(new GetCollectionJob_1.default({
            contractAddress: payload.contractAddress,
            tokenId: undefined,
            cursorIndex: undefined,
            next: undefined,
            force: payload.force ? payload.force : false,
            done: done
        }, cp, web3));
    }, 3);
    mqInstance.consume(String(process.env.MQ_QUEUE_ASSETS), (payload, done) => __awaiter(void 0, void 0, void 0, function* () {
        Logger_1.logger.info("[Terminal Asset Recoder] [Consume GetAssetsJob]");
        const cursorIndex = payload.cursorIndex ? payload.cursorIndex : 0;
        const cursorResult = yield DeduplicationManager_1.Deduplication.checkCache(payload.contractAddress, cursorIndex);
        if (cursorResult) {
            Logger_1.logger.info(`[Terminal Asset Recoder] [GetAssets] [${payload.contractAddress}] Next:${payload.next} Cache:${cursorIndex}`);
            jobManager.addJob(new GetAssetsJob_1.default({
                contractAddress: payload.contractAddress,
                tokenId: undefined,
                cursorIndex: cursorIndex,
                next: payload.next,
                force: payload.force,
                done: done
            }, cp));
        }
        else {
            done();
        }
    }), 3);
    mqInstance.consume(String(process.env.MQ_QUEUE_RARITY), (payload, done) => {
        Logger_1.logger.info("[Terminal Asset Recoder] [Consume RarityJob]");
        jobManager.addJob(new CalcRarityJob_1.default({
            contractAddress: payload.contractAddress,
            tokenId: undefined,
            cursorIndex: undefined,
            next: undefined,
            done: done
        }, cp));
    }, 150);
    // mqInstance.consume(String(process.env.MQ_QUEUE_ASSET), (payload: any, done: () => void) => {
    //     // Do not Has actions
    // }, 2)
}))();
