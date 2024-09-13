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
const libs_job_manager_1 = require("libs-job-manager");
const sql_template_strings_1 = __importDefault(require("sql-template-strings"));
const waait_1 = __importDefault(require("waait"));
const Logger_1 = require("../../utils/Logger");
const Utils_1 = require("../../utils/Utils");
class GetCollectionJob extends libs_job_manager_1.Job {
    constructor(request, cp, web3) {
        super(request);
        this.cp = cp;
        this.web3 = web3;
        this.retryLimit = 5;
        this.retryCount = 0;
    }
    getContractInfoFromOpensea(contractAddress) {
        return __awaiter(this, void 0, void 0, function* () {
            const httpResult = yield Utils_1.httpRequestCollection.get(`https://api.opensea.io/api/v1/asset_contract/${contractAddress}`);
            if (httpResult && httpResult.data) {
                const dataset = httpResult.data;
                return {
                    ADDRESS: this.request.contractAddress,
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
    getContractInfoFromDatabse(contractAddress) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.cp.readerQuerySingle("SELECT * FROM CONTRACTS WHERE `ADDRESS` = ?", [contractAddress]);
        });
    }
    getContractInfoFromNode(contractAddress) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const name = yield (new this.web3.eth.Contract((0, Utils_1.getNameABI)(), contractAddress)).methods.name().call();
                return {
                    ADDRESS: contractAddress,
                    NAME: name,
                    NETWORK: 'Ethereum',
                };
            }
            catch (e) {
                return undefined;
            }
        });
    }
    checkRequireCollect(contractAddress) {
        return __awaiter(this, void 0, void 0, function* () {
            const isExist = yield this.cp.readerQuerySingle((0, sql_template_strings_1.default) `SELECT * FROM CONTRACTS WHERE CONTRACTS.ADDRESS = ${contractAddress} AND ( DATE_ADD(UPDATED_TIME, INTERVAL 6 HOUR) >= NOW() OR CONTRACTS.SLUG IS NULL );`);
            return isExist == undefined;
        });
    }
    upsertContractInfo(contract) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.cp.writerQuery((0, sql_template_strings_1.default) `
                INSERT INTO 
                    CONTRACTS (
                        CONTRACTS.ADDRESS,
                        CONTRACTS.SCHEMA,
                        CONTRACTS.NETWORK,
                        CONTRACTS.CONTRACT_TYPE,
                        CONTRACTS.NAME,
                        CONTRACTS.NFT_VERSION,
                        CONTRACTS.IMAGE_URL,
                        CONTRACTS.SYMBOL,
                        CONTRACTS.TOTAL_SUPPLY,
                        CONTRACTS.DESCRIPTION,
                        CONTRACTS.EXTERNAL_LINK,
                        CONTRACTS.HAS_COLLECTION,
                        CONTRACTS.BANNER_IMAGE_URL,
                        CONTRACTS.COLLECTION_EXTERNAL_URL,
                        CONTRACTS.COLLECTION_IMAGE_URL,
                        CONTRACTS.FEATURED_IMAGE_URL,
                        CONTRACTS.LARGE_IMAGE_URL,
                        CONTRACTS.COLLECTION_NAME,
                        CONTRACTS.SLUG,
                        CONTRACTS.DISCORD_URL,
                        CONTRACTS.TELEGRAM_URL,
                        CONTRACTS.TWITTER_USERNAME,
                        CONTRACTS.INSTAGRAM_USERNAME,
                        CONTRACTS.WIKI_URL 
                    )
                VALUES
                    (
                        ${contract.ADDRESS},
                        ${contract.SCHEMA},
                        ${contract.NETWORK},
                        ${contract.CONTRACT_TYPE},
                        ${contract.NAME},
                        ${contract.NFT_VERSION},
                        ${contract.IMAGE_URL},
                        ${contract.SYMBOL},
                        ${contract.TOTAL_SUPPLY},
                        ${contract.DESCRIPTION},
                        ${contract.EXTERNAL_LINK},
                        ${contract.HAS_COLLECTION},
                        ${contract.BANNER_IMAGE_URL},
                        ${contract.COLLECTION_EXTERNAL_URL},
                        ${contract.COLLECTION_IMAGE_URL},
                        ${contract.FEATURED_IMAGE_URL},
                        ${contract.LARGE_IMAGE_URL},
                        ${contract.COLLECTION_NAME},
                        ${contract.SLUG},
                        ${contract.DISCORD_URL},
                        ${contract.TELEGRAM_URL},
                        ${contract.TWITTER_USERNAME},
                        ${contract.INSTAGRAM_USERNAME},
                        ${contract.WIKI_URL}
                    )
                ON DUPLICATE KEY UPDATE 

                    CONTRACTS.ADDRESS=VALUES(CONTRACTS.ADDRESS),
                    CONTRACTS.SCHEMA=VALUES(CONTRACTS.SCHEMA),
                    CONTRACTS.NETWORK=VALUES(CONTRACTS.NETWORK),
                    CONTRACTS.CONTRACT_TYPE=VALUES(CONTRACTS.CONTRACT_TYPE),
                    CONTRACTS.NAME=VALUES(CONTRACTS.NAME),
                    CONTRACTS.NFT_VERSION=VALUES(CONTRACTS.NFT_VERSION),
                    CONTRACTS.IMAGE_URL=VALUES(CONTRACTS.IMAGE_URL),
                    CONTRACTS.SYMBOL=VALUES(CONTRACTS.SYMBOL),
                    CONTRACTS.TOTAL_SUPPLY=VALUES(CONTRACTS.TOTAL_SUPPLY),
                    CONTRACTS.DESCRIPTION=VALUES(CONTRACTS.DESCRIPTION),
                    CONTRACTS.EXTERNAL_LINK=VALUES(CONTRACTS.EXTERNAL_LINK),
                    CONTRACTS.HAS_COLLECTION=VALUES(CONTRACTS.HAS_COLLECTION),
                    CONTRACTS.BANNER_IMAGE_URL=VALUES(CONTRACTS.BANNER_IMAGE_URL),
                    CONTRACTS.COLLECTION_EXTERNAL_URL=VALUES(CONTRACTS.COLLECTION_EXTERNAL_URL),
                    CONTRACTS.COLLECTION_IMAGE_URL=VALUES(CONTRACTS.COLLECTION_IMAGE_URL),
                    CONTRACTS.FEATURED_IMAGE_URL=VALUES(CONTRACTS.FEATURED_IMAGE_URL),
                    CONTRACTS.LARGE_IMAGE_URL=VALUES(CONTRACTS.LARGE_IMAGE_URL),
                    CONTRACTS.COLLECTION_NAME=VALUES(CONTRACTS.COLLECTION_NAME),
                    CONTRACTS.SLUG=VALUES(CONTRACTS.SLUG),
                    CONTRACTS.DISCORD_URL=VALUES(CONTRACTS.DISCORD_URL),
                    CONTRACTS.TELEGRAM_URL=VALUES(CONTRACTS.TELEGRAM_URL),
                    CONTRACTS.TWITTER_USERNAME=VALUES(CONTRACTS.TWITTER_USERNAME),
                    CONTRACTS.INSTAGRAM_USERNAME=VALUES(CONTRACTS.INSTAGRAM_USERNAME),
                    CONTRACTS.WIKI_URL=VALUES(CONTRACTS.WIKI_URL)
            `);
                return true;
            }
            catch (e) {
                console.log(e);
                return false;
            }
        });
    }
    getStats(slug) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const httpResult = yield Utils_1.httpRequestCollection.get(`https://api.opensea.io/api/v1/collection/${slug}/stats`);
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
                else {
                    console.log('you shall not pass');
                }
            }
            catch (e) {
                return undefined;
            }
            return undefined;
        });
    }
    checkRequireGetAssets(contract) {
        return __awaiter(this, void 0, void 0, function* () {
            if (contract.SLUG) {
                const stats = yield this.getStats(contract.SLUG);
                if (stats) {
                    try {
                        const queryResult = yield this.cp.readerQuerySingle((0, sql_template_strings_1.default) `SELECT COLLECTED_ASSETS, COLLECTING_ASSETS FROM CONTRACTS WHERE CONTRACTS.ADDRESS = ${contract.ADDRESS}`);
                        return queryResult != undefined
                            && (queryResult.COLLECTED_ASSETS != undefined && Boolean(!queryResult.COLLECTED_ASSETS))
                            && (queryResult.COLLECTING_ASSETS != undefined && Boolean(!queryResult.COLLECTING_ASSETS))
                            && stats.total_volume > 500;
                    }
                    catch (error) {
                        console.log(error, 'Error');
                        console.log(contract.SLUG, contract.ADDRESS, stats, 'error');
                    }
                }
            }
            return false;
        });
    }
    execute() {
        return __awaiter(this, void 0, void 0, function* () {
            Logger_1.logger.info(`[Terminal Asset Recoder] [Execute GetCollection] : ${this.request.contractAddress}`);
            const startTime = new Date().getTime();
            if (this.request.contractAddress == undefined) {
                // ContractAddress is Undefined
                return {
                    success: false,
                    type: "Collection",
                    done: this.request.done,
                    error: {
                        errorCode: 0,
                        errorMsg: "ContractAddress is undefined"
                    },
                };
            }
            if ((yield this.checkRequireCollect(this.request.contractAddress)) || this.request.force == true) {
                // Step 1. Get ContractInfo from Ethereum Node
                const contractInfoFromNode = yield this.getContractInfoFromNode(this.request.contractAddress);
                if (contractInfoFromNode == undefined) {
                    return {
                        success: false,
                        type: "Collection",
                        done: this.request.done,
                        error: {
                            errorCode: -56,
                            errorMsg: "Not Has ERC721 Get Name"
                        }
                    };
                }
                // Step 2. Get ContractInfo from Database
                const contractInfoFromDatabse = yield this.getContractInfoFromDatabse(this.request.contractAddress);
                // Step 3. Get ContractInfo from Opensea
                try {
                    const contractInfoFromOpensea = yield this.getContractInfoFromOpensea(this.request.contractAddress);
                    // Step 4. Merge ContractInfo
                    const contractInfo = Object.assign(Object.assign(Object.assign({}, (contractInfoFromNode ? contractInfoFromNode : {})), (contractInfoFromDatabse ? contractInfoFromDatabse : {})), (contractInfoFromOpensea ? contractInfoFromOpensea : {}));
                    if (contractInfo.ADDRESS && contractInfo.NAME) {
                        // Step 5. Upsert Contract Info
                        const result = yield this.upsertContractInfo(contractInfo);
                        if (result) {
                            const checkRequireGetAssets = yield this.checkRequireGetAssets(contractInfo);
                            // Check if 2 seconds have elapsed
                            const duration = new Date().getTime() - startTime;
                            if (duration < 2000) {
                                yield (0, waait_1.default)(2000 - duration);
                            }
                            return {
                                success: true,
                                type: "Collection",
                                done: this.request.done,
                                contract: contractInfo,
                                requestAssets: checkRequireGetAssets
                            };
                        }
                        else {
                            return {
                                success: false,
                                type: "Collection",
                                done: this.request.done,
                                error: {
                                    errorCode: 0,
                                    errorMsg: "Failed Upsert for Database"
                                },
                            };
                        }
                    }
                    else {
                        return {
                            success: false,
                            type: "Collection",
                            done: this.request.done,
                            error: {
                                errorCode: 0,
                                errorMsg: "Collection lack of information"
                            },
                        };
                    }
                }
                catch (e) {
                    // Error: 429 is Too many request
                    if (e.response) {
                        if (parseInt(e.response.status) == 429) {
                            Logger_1.logger.error(`[Terminal Asset Recoder] [GetCollection] [${this.request.contractAddress}] 429 Error`);
                            yield (0, waait_1.default)(3500 * (Math.random() + 1));
                            this.retryCount++;
                            if (this.retryLimit > this.retryCount) {
                                return yield this.execute();
                            }
                            else {
                                return {
                                    success: false,
                                    type: "Collection",
                                    done: this.request.done,
                                    error: {
                                        errorCode: -3,
                                        errorMsg: "Request Error"
                                    }
                                };
                            }
                        }
                        else {
                            return {
                                success: false,
                                type: "Collection",
                                done: this.request.done,
                                error: {
                                    errorCode: -3,
                                    errorMsg: "Request Error"
                                }
                            };
                        }
                    }
                    return {
                        success: false,
                        type: "Collection",
                        done: this.request.done,
                        error: {
                            errorCode: -3,
                            errorMsg: "Request Error"
                        }
                    };
                }
            }
            else {
                return {
                    success: false,
                    type: "Collection",
                    done: this.request.done,
                    error: {
                        errorCode: 0,
                        errorMsg: "this contract none require collection"
                    },
                };
            }
        });
    }
}
exports.default = GetCollectionJob;
