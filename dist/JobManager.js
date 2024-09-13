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
const awsprovider_1 = require("awsprovider");
const connectpool_1 = __importDefault(require("libs-connection-pool"));
const dotenv_1 = __importDefault(require("dotenv"));
const web3_1 = __importDefault(require("web3"));
const GetAsset_1 = __importDefault(require("./jobs/opensea/GetAsset"));
const GetAssets_1 = __importDefault(require("./jobs/opensea/GetAssets"));
const GetCollection_1 = __importDefault(require("./jobs/opensea/GetCollection"));
const CalcRarity_1 = __importDefault(require("./jobs/rarity/CalcRarity"));
const Logger_1 = __importDefault(require("./logger/Logger"));
dotenv_1.default.config();
class JobManager {
    constructor() {
        var _a, _b;
        this.getAssetsCache = new Map();
        Logger_1.default.info("[Terminal Asset Recoder] [Start Initializing]");
        this.CP = new connectpool_1.default({
            host: process.env.MYSQL_HOST,
            writerHost: process.env.MYSQL_HOST,
            readerHost: process.env.MYSQL_RO_HOST,
            user: process.env.MYSQL_USER,
            password: process.env.MYSQL_PASSWORD,
            port: (_a = parseInt(process.env.MYSQL_PORT)) !== null && _a !== void 0 ? _a : 3306,
            database: process.env.MYSQL_DATABASE,
            connectionLimit: (_b = parseInt(process.env.MYSQL_CONNECTION_LIMIT)) !== null && _b !== void 0 ? _b : 150
        });
        this.web3 = new web3_1.default(awsprovider_1.Provider.from({
            type: awsprovider_1.ProviderTypes.HTTP,
            endpoint: String(process.env.HTTPS_END_POINT),
            accessKeyId: String(process.env.ACCESS_KEY_ID),
            secretAccessKey: String(process.env.SECRET_ACCESS_KEY),
        }));
        Logger_1.default.info("[Terminal Asset Recoder] [Ready for database]");
    }
    setMQInstance(instance) {
        this.instance = instance;
    }
    addGetCollection(jobRequest, done) {
        return __awaiter(this, void 0, void 0, function* () {
            if (jobRequest.contractAddress) {
                Logger_1.default.info(`[Terminal Asset Recoder] [GetCollection] [${jobRequest.contractAddress}]`);
                const jobResult = yield new GetCollection_1.default(this.CP, this.web3, jobRequest).execute();
                if (jobResult.success) {
                    if (jobResult.result.requestAssets == true) {
                        if (this.instance) {
                            this.instance.publish(process.env.MQ_QUEUE_ASSETS, {
                                contractAddress: jobRequest.contractAddress,
                                bulk: 0
                            });
                        }
                    }
                }
            }
            done();
        });
    }
    addGetAssetJob(jobRequest, done) {
        return __awaiter(this, void 0, void 0, function* () {
            if (jobRequest.contractAddress && jobRequest.tokenId) {
                Logger_1.default.info(`[Terminal Asset Recoder] [GetAsset] [${jobRequest.contractAddress}] TokenId:${jobRequest.tokenId}`);
                const jobResult = yield new GetAsset_1.default(this.CP, jobRequest).execute();
                done();
            }
            else {
                done();
            }
        });
    }
    addGetAssetsJob(jobRequest, done) {
        return __awaiter(this, void 0, void 0, function* () {
            if (jobRequest.force === true) {
                this.getAssetsCache.delete(jobRequest.contractAddress);
            }
            if (jobRequest.cursorIndex == undefined) {
                jobRequest.cursorIndex = 0;
            }
            if (jobRequest.contractAddress) {
                if (this.getAssetsCache.get(jobRequest.contractAddress) == undefined || (this.getAssetsCache.get(jobRequest.contractAddress) != undefined && this.getAssetsCache.get(jobRequest.contractAddress) < jobRequest.cursorIndex)) {
                    this.getAssetsCache.set(jobRequest.contractAddress, jobRequest.cursorIndex);
                    Logger_1.default.info(`[Terminal Asset Recoder] [GetAssets] [${jobRequest.contractAddress}] Next:${jobRequest.next} Cache:${this.getAssetsCache.get(jobRequest.contractAddress)}`);
                    const jobResult = yield new GetAssets_1.default(this.CP, {
                        contractAddress: jobRequest.contractAddress,
                        tokenId: undefined,
                        next: jobRequest.next,
                        cursorIndex: jobRequest.cursorIndex
                    }).execute();
                    if (jobResult.success) {
                        if (jobResult.result.next) {
                            if (this.instance) {
                                this.instance.publish(process.env.MQ_QUEUE_ASSETS, {
                                    contractAddress: jobRequest.contractAddress,
                                    next: jobResult.result.next,
                                    cursorIndex: this.getAssetsCache.get(jobRequest.contractAddress) + 1
                                });
                            }
                        }
                        else {
                            if (this.instance) {
                                this.getAssetsCache.delete(jobRequest.contractAddress);
                                this.instance.publish(process.env.MQ_QUEUE_RARITY, {
                                    contractAddress: jobRequest.contractAddress,
                                });
                            }
                        }
                    }
                }
            }
            done();
        });
    }
    addRarityJob(jobRequest, done) {
        return __awaiter(this, void 0, void 0, function* () {
            if (jobRequest.contractAddress) {
                Logger_1.default.info(`[Terminal Asset Recoder] [CalcRarity] [${jobRequest.contractAddress}] New Job`);
                const jobResult = yield new CalcRarity_1.default(this.CP, {
                    contractAddress: jobRequest.contractAddress
                }).execute();
                if (jobResult.success) {
                    Logger_1.default.info(`[Terminal Asset Recoder] [CalcRarity] [${jobRequest.contractAddress}] Done`);
                }
            }
            done();
        });
    }
}
exports.default = JobManager;
