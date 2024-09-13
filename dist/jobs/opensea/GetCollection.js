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
const axios_1 = __importDefault(require("axios"));
const dotenv_1 = __importDefault(require("dotenv"));
const https_1 = __importDefault(require("https"));
const waait_1 = __importDefault(require("waait"));
const Job_1 = __importDefault(require("../../classes/Job"));
dotenv_1.default.config();
const httpRequest = axios_1.default.create({
    timeout: 60000,
    httpsAgent: new https_1.default.Agent({ keepAlive: true }),
    headers: { 'Content-Type': 'application/json', 'X-API-KEY': process.env.OPENSEA_API_KEY2 }
});
class GetCollection extends Job_1.default {
    constructor(CP, web3, request) {
        super(request);
        this.retryLimit = 5;
        this.CP = CP;
        this.web3 = web3;
    }
    getCollectionFromOpensea(contractAddress) {
        return __awaiter(this, void 0, void 0, function* () {
            const httpResult = yield httpRequest.get(`https://api.opensea.io/api/v1/asset_contract/${contractAddress}`);
            if (httpResult && httpResult.data) {
                const dataset = httpResult.data;
                return {
                    ADDRESS: this.jobId.contractAddress,
                    SCHEMA: dataset.schema_name,
                    NETWORK: 'Ethereum',
                    CONTRACT_TYPE: dataset.asset_contract_type,
                    NAME: dataset.name,
                    NFT_VERSION: dataset.nft_version,
                    IMAGE_URL: dataset.image_url,
                    SYMBOL: dataset.symbol,
                    TOTAL_SUPPLY: dataset.total_supply,
                    DESCRIPTION: dataset.description,
                    EXTERNAL_LINK: dataset.external_link,
                    HAS_COLLECTION: dataset.collection ? true : false,
                    BANNER_IMAGE_URL: dataset.collection ? dataset.collection.banner_image_url : undefined,
                    COLLECTION_EXTERNAL_URL: dataset.collection ? dataset.collection.external_link : undefined,
                    COLLECTION_IMAGE_URL: dataset.collection ? dataset.collection.image_url : undefined,
                    FEATURED_IMAGE_URL: dataset.collection ? dataset.collection.featured_image_url : undefined,
                    LARGE_IMAGE_URL: dataset.collection ? dataset.collection.large_image_url : undefined,
                    COLLECTION_NAME: dataset.collection ? dataset.collection.name : undefined,
                    SLUG: dataset.collection ? dataset.collection.slug : undefined,
                    DISCORD_URL: dataset.collection ? dataset.collection.discord_url : undefined,
                    TELEGRAM_URL: dataset.collection ? dataset.collection.telegram_url : undefined,
                    TWITTER_USERNAME: dataset.collection ? dataset.collection.twitter_username : undefined,
                    INSTAGRAM_USERNAME: dataset.collection ? dataset.collection.instagram_username : undefined,
                    WIKI_URL: dataset.collection ? dataset.collection.wiki_url : undefined,
                };
            }
            return undefined;
        });
    }
    getCollectionFromDatabse(contractAddress) {
        return __awaiter(this, void 0, void 0, function* () {
            const resultFromDatabse = yield this.CP.readerQuery("SELECT * FROM CONTRACTS WHERE `ADDRESS` = ?", [contractAddress]);
            return resultFromDatabse.length > 0 ? {
                ADDRESS: contractAddress,
                SCHEMA: resultFromDatabse[0].SCHEMA,
                NETWORK: 'Ethereum',
                CONTRACT_TYPE: resultFromDatabse[0].CONTRACT_TYPE,
                NAME: resultFromDatabse[0].NAME,
                NFT_VERSION: resultFromDatabse[0].NFT_VERSION,
                IMAGE_URL: resultFromDatabse[0].IMAGE_URL,
                SYMBOL: resultFromDatabse[0].SYMBOL,
                TOTAL_SUPPLY: resultFromDatabse[0].TOTAL_SUPPLY,
                DESCRIPTION: resultFromDatabse[0].DESCRIPTION,
                EXTERNAL_LINK: resultFromDatabse[0].EXTERNAL_LINK,
                HAS_COLLECTION: resultFromDatabse[0].HAS_COLLECTION,
                BANNER_IMAGE_URL: resultFromDatabse[0].BANNER_IMAGE_URL,
                COLLECTION_EXTERNAL_URL: resultFromDatabse[0].COLLECTION_EXTERNAL_URL,
                COLLECTION_IMAGE_URL: resultFromDatabse[0].COLLECTION_IMAGE_URL,
                FEATURED_IMAGE_URL: resultFromDatabse[0].FEATURED_IMAGE_URL,
                LARGE_IMAGE_URL: resultFromDatabse[0].LARGE_IMAGE_URL,
                COLLECTION_NAME: resultFromDatabse[0].COLLECTION_NAME,
                SLUG: resultFromDatabse[0].SLUG,
                DISCORD_URL: resultFromDatabse[0].DISCORD_URL,
                TELEGRAM_URL: resultFromDatabse[0].TELEGRAM_URL,
                TWITTER_USERNAME: resultFromDatabse[0].TWITTER_USERNAME,
                INSTAGRAM_USERNAME: resultFromDatabse[0].INSTAGRAM_USERNAME,
                WIKI_URL: resultFromDatabse[0].WIKI_URL,
            } : {
                ADDRESS: contractAddress,
            };
        });
    }
    getCollectionFromNode(contractAddress) {
        return __awaiter(this, void 0, void 0, function* () {
            const NAME_ABI = [
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
            const name = yield (new this.web3.eth.Contract(NAME_ABI, contractAddress)).methods.name().call();
            return {
                ADDRESS: contractAddress,
                NAME: name,
                NETWORK: 'Ethereum',
            };
        });
    }
    upsertToDatabase(collection) {
        return __awaiter(this, void 0, void 0, function* () {
            const keys = [
                "ADDRESS",
                "SCHEMA",
                "NETWORK",
                "CONTRACT_TYPE",
                "NAME",
                "NFT_VERSION",
                "IMAGE_URL",
                "SYMBOL",
                "TOTAL_SUPPLY",
                "DESCRIPTION",
                "EXTERNAL_LINK",
                "HAS_COLLECTION",
                "BANNER_IMAGE_URL",
                "COLLECTION_EXTERNAL_URL",
                "COLLECTION_IMAGE_URL",
                "FEATURED_IMAGE_URL",
                "LARGE_IMAGE_URL",
                "COLLECTION_NAME",
                "SLUG",
                "DISCORD_URL",
                "TELEGRAM_URL",
                "TWITTER_USERNAME",
                "INSTAGRAM_USERNAME",
                "WIKI_URL"
            ].map(e => "`" + e + "`");
            const values = [
                collection.ADDRESS,
                collection.SCHEMA,
                collection.NETWORK,
                collection.CONTRACT_TYPE,
                collection.NAME,
                collection.NFT_VERSION,
                collection.IMAGE_URL,
                collection.SYMBOL,
                collection.TOTAL_SUPPLY,
                collection.DESCRIPTION,
                collection.EXTERNAL_LINK,
                collection.HAS_COLLECTION ? 1 : 0,
                collection.BANNER_IMAGE_URL,
                collection.COLLECTION_EXTERNAL_URL,
                collection.COLLECTION_IMAGE_URL,
                collection.FEATURED_IMAGE_URL,
                collection.LARGE_IMAGE_URL,
                collection.COLLECTION_NAME,
                collection.SLUG,
                collection.DISCORD_URL,
                collection.TELEGRAM_URL,
                collection.TWITTER_USERNAME,
                collection.INSTAGRAM_USERNAME,
                collection.WIKI_URL
            ];
            const connection = yield this.CP.beginTransaction();
            try {
                yield this.CP.query(connection, `INSERT INTO CONTRACTS (${keys.join(",")}) VALUES (${new Array(values.length).fill("?").join(",")}) ` + "ON DUPLICATE KEY UPDATE `ADDRESS`=VALUES(`ADDRESS`),`SCHEMA`=VALUES(`SCHEMA`),`NETWORK`=VALUES(`NETWORK`),`CONTRACT_TYPE`=VALUES(`CONTRACT_TYPE`),`NAME`=VALUES(`NAME`),`NFT_VERSION`=VALUES(`NFT_VERSION`),`IMAGE_URL`=VALUES(`IMAGE_URL`),`SYMBOL`=VALUES(`SYMBOL`),`TOTAL_SUPPLY`=VALUES(`TOTAL_SUPPLY`),`DESCRIPTION`=VALUES(`DESCRIPTION`),`EXTERNAL_LINK`=VALUES(`EXTERNAL_LINK`),`HAS_COLLECTION`=VALUES(`HAS_COLLECTION`),`BANNER_IMAGE_URL`=VALUES(`BANNER_IMAGE_URL`),`COLLECTION_EXTERNAL_URL`=VALUES(`COLLECTION_EXTERNAL_URL`),`COLLECTION_IMAGE_URL`=VALUES(`COLLECTION_IMAGE_URL`),`FEATURED_IMAGE_URL`=VALUES(`FEATURED_IMAGE_URL`),`LARGE_IMAGE_URL`=VALUES(`LARGE_IMAGE_URL`),`COLLECTION_NAME`=VALUES(`COLLECTION_NAME`),`SLUG`=VALUES(`SLUG`),`DISCORD_URL`=VALUES(`DISCORD_URL`),`TELEGRAM_URL`=VALUES(`TELEGRAM_URL`),`TWITTER_USERNAME`=VALUES(`TWITTER_USERNAME`),`INSTAGRAM_USERNAME`=VALUES(`INSTAGRAM_USERNAME`),`WIKI_URL`=VALUES(`WIKI_URL`)", [...values]);
                yield this.CP.commit(connection);
                connection.release();
            }
            catch (e) {
                connection.rollback(() => {
                    connection.release();
                });
                //Logger.error(`[Terminal Asset Recoder] [GetCollection] [${this.jobId.contractAddress}] ${(e as any).message}`);
                return false;
            }
            return true;
        });
    }
    checkRequireCollect(contractAddress) {
        return __awaiter(this, void 0, void 0, function* () {
            const isExist = yield this.CP.readerQuery("SELECT * FROM CONTRACTS WHERE `ADDRESS` = ?", [contractAddress]);
            const isExistButNotOpensea = yield this.CP.readerQuery("SELECT * FROM CONTRACTS WHERE `ADDRESS` = ? AND ( UPDATED_TIME <= DATE_SUB(NOW(), INTERVAL 12 HOUR) OR `SLUG` IS NULL )", [contractAddress]);
            return isExist.length == 0 || (isExist.length > 0 && isExistButNotOpensea.length > 0);
        });
    }
    checkRequireGetAssets(collection) {
        return __awaiter(this, void 0, void 0, function* () {
            if (collection.SLUG) {
                const stats = yield this.getStats(collection.SLUG);
                const queryResult = yield this.CP.readerQuery("SELECT COLLECTED_ASSETS, COLLECTING FROM CONTRACTS WHERE ADDRESS = ?;", [this.jobId.contractAddress]);
                return queryResult.length > 0 && queryResult[0].COLLECTED_ASSETS == 0 && queryResult[0].COLLECTING == 0 && stats.total_volume > 500;
            }
            return false;
        });
    }
    getStats(slug) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const httpResult = yield httpRequest.get(`https://api.opensea.io/api/v1/collection/${slug}/stats`);
                if (httpResult && httpResult.data && httpResult.data.stats) {
                    const stats = httpResult.data.stats;
                    return {
                        "one_day_volume": stats.one_day_volume,
                        "one_day_change": stats.one_day_change,
                        "one_day_sales": stats.one_day_sales,
                        "one_day_average_price": stats.one_day_average_price,
                        "seven_day_volume": stats.seven_day_volume,
                        "seven_day_change": stats.seven_day_change,
                        "seven_day_sales": stats.seven_day_sales,
                        "seven_day_average_price": stats.seven_day_average_price,
                        "thirty_day_volume": stats.thirty_day_volume,
                        "thirty_day_change": stats.thirty_day_change,
                        "thirty_day_sales": stats.thirty_day_sales,
                        "thirty_day_average_price": stats.thirty_day_average_price,
                        "total_volume": stats.total_volume,
                        "total_sales": stats.total_sales,
                        "total_supply": stats.total_supply,
                        "count": stats.count,
                        "num_owners": stats.num_owners,
                        "average_price": stats.average_price,
                        "num_reports": stats.num_reports,
                        "market_cap": stats.market_cap,
                        "floor_price": stats.floor_price
                    };
                }
            }
            catch (e) {
                //Logger.error(`[Terminal Asset Recoder] [GetCollection] [${this.jobId.contractAddress}] ${(e as any).message}`);
                return undefined;
            }
            return undefined;
        });
    }
    execute(retry = 0) {
        return __awaiter(this, void 0, void 0, function* () {
            const startTime = new Date().getTime();
            if (this.jobId.contractAddress == undefined) {
                return {
                    success: false,
                    result: {
                        errorCode: 0,
                        errorMsg: "ContractAddress is undefined"
                    },
                };
            }
            if (yield this.checkRequireCollect(this.jobId.contractAddress)) {
                try {
                    // Step1 : Get Name from Blockcahin Node
                    let collectionFromNode = undefined;
                    try {
                        //Logger.info(`[Terminal Asset Recoder] [GetCollection] [${this.jobId.contractAddress}] Step1 : Get Name from Blockcahin Node`);
                        collectionFromNode = yield this.getCollectionFromNode(this.jobId.contractAddress);
                    }
                    catch (e) {
                        //Logger.error(`[Terminal Asset Recoder] [GetCollection] [${this.jobId.contractAddress}] ${(e as any).message}`);
                        return {
                            success: false,
                            result: {
                                errorCode: -56,
                                errorMsg: "Not Has ERC721 Get Name"
                            }
                        };
                    }
                    // Step2 : Get Collection Information from Opensea
                    //Logger.info(`[Terminal Asset Recoder] [GetCollection] [${this.jobId.contractAddress}] Step2 : Get Collection Information from Opensea`);
                    let collectionFromOpensea = undefined;
                    try {
                        collectionFromOpensea = yield this.getCollectionFromOpensea(this.jobId.contractAddress);
                    }
                    catch (error) {
                        if (parseInt(error.response.status) == 429) {
                            //Logger.error(`[Terminal Asset Recoder] [GetCollection] [${this.jobId.contractAddress}] 409 Error`);
                            yield (0, waait_1.default)(3500);
                        }
                    }
                    const collectionFromDatabse = yield this.getCollectionFromDatabse(this.jobId.contractAddress);
                    const collection = Object.assign(Object.assign(Object.assign({}, (collectionFromNode ? collectionFromNode : {})), (collectionFromDatabse ? collectionFromDatabse : {})), (collectionFromOpensea ? collectionFromOpensea : {}));
                    if (collection.ADDRESS && collection.NAME) {
                        // Step3 : Upsert collection to Database
                        //Logger.info(`[Terminal Asset Recoder] [GetCollection] [${this.jobId.contractAddress}] Step3 : Upsert collection to Database`);
                        const result = yield this.upsertToDatabase(collection);
                        if (result) {
                            const duration = new Date().getTime() - startTime;
                            if (duration < 2000) {
                                yield (0, waait_1.default)(2000 - duration);
                            }
                            //Logger.info(`[Terminal Asset Recoder] [GetCollection] [${this.jobId.contractAddress}] Done / ${duration}ms`);
                            const requestAssets = yield this.checkRequireGetAssets(collection);
                            return {
                                success: true,
                                result: {
                                    collection: collection,
                                    requestAssets: requestAssets,
                                },
                            };
                        }
                    }
                    const duration = new Date().getTime() - startTime;
                    if (duration < 2000) {
                        yield (0, waait_1.default)(2000 - duration);
                    }
                    return {
                        success: true,
                        result: {
                            errorCode: -2,
                            errorMsg: "Collection is undefined"
                        }
                    };
                }
                catch (error) {
                    if (error.response) {
                        // 요청이 이루어졌으며 서버가 2xx의 범위를 벗어나는 상태 코드로 응답했습니다.
                        // //Logger.info(error.response.data);
                        // //Logger.info(error.response.status);
                        // //Logger.info(error.response.headers);
                        //Logger.error(`[MicroJob Error] Response Code: ${error.response.status} - ${error.response.data}`);
                        if (parseInt(error.response.status) == 429) {
                            //Logger.error(`[MicroJob Error] Retry: ${retry}`);
                            yield (0, waait_1.default)(3500 * (Math.random() + 1));
                            return yield this.execute(++retry);
                        }
                        else {
                            const duration = new Date().getTime() - startTime;
                            if (duration < 2000) {
                                yield (0, waait_1.default)(2000 - duration);
                            }
                            return {
                                success: false,
                                result: {
                                    errorCode: -3,
                                    errorMsg: "Request Error"
                                }
                            };
                        }
                    }
                    else if (error.request) {
                        // 요청이 이루어 졌으나 응답을 받지 못했습니다.
                        // `error.request`는 브라우저의 XMLHttpRequest 인스턴스 또는
                        // Node.js의 http.ClientRequest 인스턴스입니다.
                        //Logger.error(`[MicroJob Error] Request Error`);
                        const duration = new Date().getTime() - startTime;
                        if (duration < 2000) {
                            yield (0, waait_1.default)(2000 - duration);
                        }
                        return {
                            success: false,
                            result: {
                                errorCode: -3,
                                errorMsg: "Request Error"
                            }
                        };
                    }
                    else {
                        // 오류를 발생시킨 요청을 설정하는 중에 문제가 발생했습니다.
                        //Logger.error(`MicroJob Error] Undefined Error`);
                        const duration = new Date().getTime() - startTime;
                        if (duration < 2000) {
                            yield (0, waait_1.default)(2000 - duration);
                        }
                        return {
                            success: false,
                            result: {
                                errorCode: -4,
                                errorMsg: "Undefined Error"
                            }
                        };
                    }
                }
            }
            else {
                //Logger.info(`[Terminal Asset Recoder] [GetCollection] [${this.jobId.contractAddress}] Update Not Required`);
                const queryResult = yield this.CP.readerQuery("SELECT COLLECTED_ASSETS, COLLECTING FROM CONTRACTS WHERE ADDRESS = ?;", [this.jobId.contractAddress]);
                if (queryResult.length > 0 && queryResult[0].COLLECTED_ASSETS == 0 && queryResult[0].COLLECTING == 0) {
                    return {
                        success: true,
                        result: {
                            collection: {},
                            requestAssets: true,
                        },
                    };
                }
                else {
                    return {
                        success: true,
                        result: {
                            collection: {},
                            requestAssets: false,
                        },
                    };
                }
            }
        });
    }
}
exports.default = GetCollection;
