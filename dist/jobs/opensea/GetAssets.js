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
const Job_1 = __importDefault(require("../../classes/Job"));
const axios_1 = __importDefault(require("axios"));
const https_1 = __importDefault(require("https"));
const Logger_1 = __importDefault(require("../../logger/Logger"));
const waait_1 = __importDefault(require("waait"));
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
const httpRequest = axios_1.default.create({
    httpsAgent: new https_1.default.Agent({ keepAlive: false }),
    headers: { 'Content-Type': 'application/json', 'X-API-KEY': process.env.OPENSEA_API_KEY }
});
class GetAssets extends Job_1.default {
    constructor(CP, request) {
        super(request);
        this.retryLimit = 5;
        this.CP = CP;
    }
    getAssetsByCursor(contractAddress, cursor) {
        return __awaiter(this, void 0, void 0, function* () {
            let url = "";
            if (cursor) {
                url = `https://api.opensea.io/api/v1/assets?order_direction=desc&limit=50&asset_contract_address=${contractAddress}&cursor=${cursor}`;
            }
            else {
                url = `https://api.opensea.io/api/v1/assets?order_direction=desc&limit=50&asset_contract_address=${contractAddress}`;
            }
            const httpResult = yield httpRequest.get(url);
            if (httpResult && httpResult.data) {
                return {
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
                        LAST_SALE_TX_HASH: asset.last_sale ? asset.last_sale.transaction.transaction_hash : undefined,
                        LAST_SALE_PRICE: asset.last_sale ? asset.last_sale.total_price : undefined,
                        LAST_SALE_PAYMENT_TOKEN: asset.last_sale ? asset.last_sale.payment_token.address : undefined,
                        LAST_SALE_PAYMENT_TOKEN_DECIMALS: asset.last_sale ? asset.last_sale.payment_token.decimals : undefined,
                        LAST_SALE_TIME: asset.last_sale ? asset.last_sale.transaction.timestamp : undefined,
                    }))
                };
            }
            return {
                next: undefined,
                previous: undefined,
                assets: []
            };
        });
    }
    assetToDatabaseObject(asset) {
        return Object.assign(Object.assign({}, asset), {
            TRAITS: JSON.stringify(asset.TRAITS),
            LISTINGS: asset.LISTINGS ? JSON.stringify(asset.LISTINGS) : undefined
        });
    }
    upsertToDatabase(assets) {
        return __awaiter(this, void 0, void 0, function* () {
            const obejcts = assets.map(a => this.assetToDatabaseObject(a));
            const keys = [
                "ADDRESS",
                "TOKEN_ID",
                "NAME",
                "TOKEN_METADATA",
                "OWNER_ADDRESS",
                "IMAGE_URL",
                "IMAGE_PREVIEW_URL",
                "IMAGE_ORIGINAL_URL",
                "ANIMATION_URL",
                "ANIMATION_ORIGINAL_URL",
                "TRAITS",
                "LAST_SALE_TX_HASH",
                "LAST_SALE_PRICE",
                "LAST_SALE_PAYMENT_TOKEN",
                "LAST_SALE_PAYMENT_TOKEN_DECIMALS",
                "LAST_SALE_TIME",
                "LISTINGS"
            ];
            const values = obejcts.map(obejct => [
                obejct.ADDRESS,
                obejct.TOKEN_ID,
                obejct.NAME,
                obejct.TOKEN_METADATA,
                obejct.OWNER_ADDRESS,
                obejct.IMAGE_URL,
                obejct.IMAGE_PREVIEW_URL,
                obejct.IMAGE_ORIGINAL_URL,
                obejct.ANIMATION_URL,
                obejct.ANIMATION_ORIGINAL_URL,
                obejct.TRAITS,
                obejct.LAST_SALE_TX_HASH,
                obejct.LAST_SALE_PRICE,
                obejct.LAST_SALE_PAYMENT_TOKEN,
                obejct.LAST_SALE_PAYMENT_TOKEN_DECIMALS,
                obejct.LAST_SALE_TIME,
                obejct.LISTINGS
            ]);
            try {
                const connection = yield this.CP.beginTransaction();
                yield this.CP.query(connection, `INSERT INTO ASSETS (${keys.join(",")}) VALUES ${new Array(values.length).fill(`(${new Array(keys.length).fill("?").join(",")})`).join(",")} ON DUPLICATE KEY UPDATE ADDRESS=VALUES(ADDRESS), TOKEN_ID=VALUES(TOKEN_ID), NAME=VALUES(NAME), TOKEN_METADATA=VALUES(TOKEN_METADATA), OWNER_ADDRESS=VALUES(OWNER_ADDRESS), IMAGE_URL=VALUES(IMAGE_URL), IMAGE_PREVIEW_URL=VALUES(IMAGE_PREVIEW_URL), IMAGE_ORIGINAL_URL=VALUES(IMAGE_ORIGINAL_URL), ANIMATION_URL=VALUES(ANIMATION_URL), ANIMATION_ORIGINAL_URL=VALUES(ANIMATION_ORIGINAL_URL), TRAITS=VALUES(TRAITS), LAST_SALE_TX_HASH=VALUES(LAST_SALE_TX_HASH), LAST_SALE_PRICE=VALUES(LAST_SALE_PRICE), LAST_SALE_PAYMENT_TOKEN=VALUES(LAST_SALE_PAYMENT_TOKEN), LAST_SALE_PAYMENT_TOKEN_DECIMALS=VALUES(LAST_SALE_PAYMENT_TOKEN_DECIMALS), LAST_SALE_TIME=VALUES(LAST_SALE_TIME), LISTINGS=VALUES(LISTINGS)`, [...values.flat(1)]);
                yield this.CP.commit(connection);
                connection.release();
            }
            catch (e) {
                Logger_1.default.error(`[Terminal Asset Recoder] [GetAssets] [${this.jobId.contractAddress}] ${e.message}`);
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
            Logger_1.default.info(`[Terminal Asset Recoder] [GetAssets] [${this.jobId.contractAddress}] Cursor:${cursor} / ${duration}ms`);
            if (duration < 1000) {
                yield (0, waait_1.default)(1000 - duration);
            }
            return assetsResult.next;
        });
    }
    execute(retry = 0) {
        return __awaiter(this, void 0, void 0, function* () {
            if (retry >= this.retryLimit) {
                return {
                    success: false,
                    result: {
                        errorCode: -1,
                        errorMsg: "Re-try reached its limit"
                    }
                };
            }
            try {
                yield this.CP.writerQueryNotUpdated("UPDATE CONTRACTS SET COLLECTING = ? WHERE ADDRESS = ?;", [1, this.jobId.contractAddress]);
                if (this.jobId.cursorIndex == 0) {
                    yield this.CP.writerQueryNotUpdated("INSERT COLLECT_ASSETS_LOG ( COLLECTING, REQUEST_TIME, CONTRACT_ADDRESS ) VALUES ( TRUE, CURRENT_TIMESTAMP(),?) ON DUPLICATE KEY UPDATE REQUEST_TIME = CURRENT_TIMESTAMP(), COLLECTING = 1;", [this.jobId.contractAddress]);
                }
                const nextCusor = yield this.getAssets(this.jobId.contractAddress, this.jobId.next);
                if (nextCusor == null) {
                    yield this.CP.writerQueryNotUpdated("UPDATE CONTRACTS SET COLLECTED_ASSETS = ? WHERE ADDRESS = ?;", [1, this.jobId.contractAddress]);
                    yield this.CP.writerQueryNotUpdated("UPDATE COLLECT_ASSETS_LOG SET COLLECTED_TIME = CURRENT_TIMESTAMP(), COLLECTING = 0 WHERE CONTRACT_ADDRESS = ?;", [this.jobId.contractAddress]);
                    Logger_1.default.info(`[Terminal Asset Recoder] [GetAssets] [${this.jobId.contractAddress}] Done`);
                }
                return {
                    success: true,
                    result: {
                        next: nextCusor
                    }
                };
            }
            catch (error) {
                if (error.response) {
                    // 요청이 이루어졌으며 서버가 2xx의 범위를 벗어나는 상태 코드로 응답했습니다.
                    // logger.info(error.response.data);
                    // logger.info(error.response.status);
                    // logger.info(error.response.headers);
                    Logger_1.default.error(`[GetAssets] Response Code: ${error.response.status} - ${error.response.data}`);
                    if (parseInt(error.response.status) == 429 || parseInt(error.response.status) == 503) {
                        Logger_1.default.error(`[GetAssets] Retry: ${retry}`);
                        yield (0, waait_1.default)(3500);
                        return yield this.execute(++retry);
                    }
                    else {
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
                    Logger_1.default.error(`[GetAssets] Request Error`);
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
                    Logger_1.default.error(`GetAssets] Undefined Error`);
                    return {
                        success: false,
                        result: {
                            errorCode: -4,
                            errorMsg: "Undefined Error"
                        }
                    };
                }
            }
        });
    }
}
exports.default = GetAssets;
