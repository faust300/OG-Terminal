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
const dotenv_1 = __importDefault(require("dotenv"));
const libs_job_manager_1 = require("libs-job-manager");
const waait_1 = __importDefault(require("waait"));
const Utils_1 = require("../../utils/Utils");
const Logger_1 = require("../../utils/Logger");
const sql_template_strings_1 = __importDefault(require("sql-template-strings"));
const DeduplicationManager_1 = require("../../DeduplicationManager");
dotenv_1.default.config();
class GetAssetsJob extends libs_job_manager_1.Job {
    constructor(request, cp) {
        super(request);
        this.cp = cp;
        this.retryLimit = 5;
        this.retryCount = 0;
    }
    getAssetsByCursor(contractAddress, cursor) {
        return __awaiter(this, void 0, void 0, function* () {
            let url = `https://api.opensea.io/api/v1/assets?order_direction=desc&limit=50&asset_contract_address=${contractAddress}${cursor ? `&cursor=${cursor}` : ``}`;
            const httpResult = yield Utils_1.httpRequestAssets.get(url);
            if (httpResult && httpResult.data) {
                const httpResultData = {
                    next: httpResult.data.next,
                    previous: httpResult.data.previous,
                    assets: httpResult.data.assets.map(asset => ({
                        ADDRESS: contractAddress,
                        TOKEN_ID: asset.token_id,
                        NAME: asset.name,
                        TOKEN_METADATA: asset.token_metadata,
                        OWNER_ADDRESS: asset.owner ? asset.owner.address : "0x0000000000000000000000000000000000000000",
                        IMAGE_URL: asset.image_url,
                        IMAGE_PREVIEW_URL: asset.image_url,
                        IMAGE_THUMBNAIL_URL: asset.image_thumbnail_url,
                        IMAGE_ORIGINAL_URL: asset.image_original_url,
                        ANIMATION_URL: asset.animation_url,
                        ANIMATION_ORIGINAL_URL: asset.animation_original_url,
                        TRAITS: asset.traits,
                        LAST_SALE_TX_HASH: (asset.last_sale && asset.last_sale.transaction) ? asset.last_sale.transaction.transaction_hash : undefined,
                        LAST_SALE_PRICE: asset.last_sale ? asset.last_sale.total_price : undefined,
                        LAST_SALE_PAYMENT_TOKEN: (asset.last_sale && asset.last_sale.payment_token) ? asset.last_sale.payment_token.address : undefined,
                        LAST_SALE_PAYMENT_TOKEN_DECIMALS: (asset.last_sale && asset.last_sale.payment_token) ? asset.last_sale.payment_token.decimals : undefined,
                        LAST_SALE_TIME: (asset.last_sale && asset.last_sale.transaction) ? asset.last_sale.transaction.timestamp : undefined,
                    }))
                };
                return httpResultData;
            }
            return {
                next: undefined,
                previous: undefined,
                assets: []
            };
        });
    }
    upsertToDatabase(assets) {
        return __awaiter(this, void 0, void 0, function* () {
            const queries = assets.reduce((query, asset) => {
                query.append((0, sql_template_strings_1.default) `
                INSERT INTO 
                    ASSETS (
                        ASSETS.ADDRESS,
                        ASSETS.TOKEN_ID,
                        ASSETS.NAME,
                        ASSETS.TOKEN_METADATA,
                        ASSETS.OWNER_ADDRESS,
                        ASSETS.IMAGE_URL,
                        ASSETS.IMAGE_PREVIEW_URL,
                        ASSETS.IMAGE_ORIGINAL_URL,
                        ASSETS.ANIMATION_URL,
                        ASSETS.ANIMATION_ORIGINAL_URL,
                        ASSETS.TRAITS,
                        ASSETS.LAST_SALE_TX_HASH,
                        ASSETS.LAST_SALE_PRICE,
                        ASSETS.LAST_SALE_PAYMENT_TOKEN,
                        ASSETS.LAST_SALE_PAYMENT_TOKEN_DECIMALS,
                        ASSETS.LAST_SALE_TIME,
                        ASSETS.LISTINGS
                    )
                VALUES
                    (
                        ${asset.ADDRESS},
                        ${asset.TOKEN_ID},
                        ${asset.NAME},
                        ${asset.TOKEN_METADATA},
                        ${asset.OWNER_ADDRESS},
                        ${asset.IMAGE_URL},
                        ${asset.IMAGE_PREVIEW_URL},
                        ${asset.IMAGE_ORIGINAL_URL},
                        ${asset.ANIMATION_URL},
                        ${asset.ANIMATION_ORIGINAL_URL},
                        ${JSON.stringify(asset.TRAITS)},
                        ${asset.LAST_SALE_TX_HASH},
                        ${asset.LAST_SALE_PRICE},
                        ${asset.LAST_SALE_PAYMENT_TOKEN},
                        ${asset.LAST_SALE_PAYMENT_TOKEN_DECIMALS},
                        ${asset.LAST_SALE_TIME},
                        ${JSON.stringify(asset.LISTINGS)}
                    )
                ON DUPLICATE KEY UPDATE 
                    ADDRESS=VALUES(ADDRESS), 
                    TOKEN_ID=VALUES(TOKEN_ID), 
                    NAME=VALUES(NAME), 
                    TOKEN_METADATA=VALUES(TOKEN_METADATA), 
                    OWNER_ADDRESS=VALUES(OWNER_ADDRESS), 
                    IMAGE_URL=VALUES(IMAGE_URL), 
                    IMAGE_PREVIEW_URL=VALUES(IMAGE_PREVIEW_URL), 
                    IMAGE_ORIGINAL_URL=VALUES(IMAGE_ORIGINAL_URL), 
                    ANIMATION_URL=VALUES(ANIMATION_URL), 
                    ANIMATION_ORIGINAL_URL=VALUES(ANIMATION_ORIGINAL_URL), 
                    TRAITS=VALUES(TRAITS), 
                    LAST_SALE_TX_HASH=VALUES(LAST_SALE_TX_HASH), 
                    LAST_SALE_PRICE=VALUES(LAST_SALE_PRICE), 
                    LAST_SALE_PAYMENT_TOKEN=VALUES(LAST_SALE_PAYMENT_TOKEN), 
                    LAST_SALE_PAYMENT_TOKEN_DECIMALS=VALUES(LAST_SALE_PAYMENT_TOKEN_DECIMALS), 
                    LAST_SALE_TIME=VALUES(LAST_SALE_TIME), 
                    LISTINGS=VALUES(LISTINGS);
            `);
                return query;
            }, (0, sql_template_strings_1.default) ``);
            try {
                yield this.cp.writerQuery(queries);
            }
            catch (e) {
                Logger_1.logger.error(`[Terminal Asset Recoder] [GetAssets] [${this.request.contractAddress}] ${e.message}`);
                return false;
            }
            return true;
        });
    }
    getAssets(contractAddress, cursor) {
        return __awaiter(this, void 0, void 0, function* () {
            const startTime = new Date().getTime();
            const assetsResult = yield this.getAssetsByCursor(contractAddress, cursor);
            if (assetsResult.assets.length > 0) {
                yield this.upsertToDatabase(assetsResult.assets);
            }
            const duration = new Date().getTime() - startTime;
            Logger_1.logger.info(`[Terminal Asset Recoder] [GetAssets] [${this.request.contractAddress}] Cursor:${cursor} / ${duration}ms`);
            if (duration < 1000) {
                yield (0, waait_1.default)(1000 - duration);
            }
            return assetsResult.next;
        });
    }
    execute() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.request.contractAddress == undefined) {
                // ContractAddress is Undefined
                return {
                    success: false,
                    type: "Assets",
                    done: this.request.done,
                    error: {
                        errorCode: 0,
                        errorMsg: "ContractAddress is undefined"
                    },
                };
            }
            try {
                // Update CollectingAssets to true in Contracts
                yield this.cp.writerQuery((0, sql_template_strings_1.default) `UPDATE CONTRACTS SET COLLECTING_ASSETS = ${true} WHERE ADDRESS = ${this.request.contractAddress};`);
                // TODO: Add Collecting Log
                if (this.request.cursorIndex == 0) {
                    try {
                        yield this.cp.writerQuery((0, sql_template_strings_1.default) `INSERT COLLECT_ASSETS_LOG ( COLLECTING, REQUEST_TIME, CONTRACT_ADDRESS ) VALUES ( TRUE, CURRENT_TIMESTAMP(), ${this.request.contractAddress}) ON DUPLICATE KEY UPDATE REQUEST_TIME = CURRENT_TIMESTAMP(), COLLECTING = ${true}`);
                    }
                    catch (error) {
                        console.log(error);
                    }
                }
                const nextCusor = yield this.getAssets(this.request.contractAddress, this.request.next);
                if (nextCusor == null || nextCusor == undefined) {
                    const connection = yield this.cp.beginTransaction();
                    try {
                        yield this.cp.query(connection, (0, sql_template_strings_1.default) `UPDATE CONTRACTS SET COLLECTED_ASSETS = ${true}, COLLECTING_ASSETS = ${false} WHERE ADDRESS = ${this.request.contractAddress};`);
                        yield this.cp.query(connection, (0, sql_template_strings_1.default) `UPDATE COLLECT_ASSETS_LOG SET COLLECTED_TIME = CURRENT_TIMESTAMP(), COLLECTING = ${false} WHERE CONTRACT_ADDRESS = ${this.request.contractAddress};`);
                        yield this.cp.commit(connection);
                        connection.release();
                        Logger_1.logger.info(`[Terminal Asset Recoder] [GetAssets] [${this.request.contractAddress}] Done`);
                    }
                    catch (e) {
                        connection.rollback(() => {
                            connection.release();
                        });
                    }
                }
                if (nextCusor) {
                    const updateNext = yield DeduplicationManager_1.Deduplication.updateNext(this.request.contractAddress, nextCusor);
                }
                else {
                    const updateEnd = yield DeduplicationManager_1.Deduplication.updateEndPoint(this.request.contractAddress);
                }
                return {
                    success: true,
                    type: "Assets",
                    done: this.request.done,
                    contractAddress: this.request.contractAddress,
                    next: nextCusor
                };
            }
            catch (e) {
                if (e && e.response && e.response.status && parseInt(e.response.status) == 429) {
                    Logger_1.logger.error(`[Terminal Asset Recoder] [GetAssets] [${this.request.contractAddress}] 429 Error`);
                    yield (0, waait_1.default)(3500 * (Math.random() + 1));
                    this.retryCount++;
                    if (this.retryLimit > this.retryCount) {
                        return yield this.execute();
                    }
                    else {
                        DeduplicationManager_1.Deduplication.deleteCursor(this.request.contractAddress);
                        return {
                            success: false,
                            type: "Assets",
                            done: this.request.done,
                            error: {
                                errorCode: -3,
                                errorMsg: "Request Error"
                            }
                        };
                    }
                }
                DeduplicationManager_1.Deduplication.deleteCursor(this.request.contractAddress);
                return {
                    success: false,
                    type: "Assets",
                    done: this.request.done,
                    error: {
                        errorCode: -3,
                        errorMsg: "Request Error"
                    }
                };
            }
        });
    }
}
exports.default = GetAssetsJob;
