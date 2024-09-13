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
    timeout: 60000,
    httpsAgent: new https_1.default.Agent({ keepAlive: true }),
    headers: { 'Content-Type': 'application/json', 'X-API-KEY': process.env.OPENSEA_API_KEY }
});
class GetAsset extends Job_1.default {
    constructor(CP, request) {
        super(request);
        this.retryLimit = 5;
        this.CP = CP;
    }
    getAssetsWithTokenIds(contractAddress, tokenIds) {
        return __awaiter(this, void 0, void 0, function* () {
            const tokenIdsQueries = tokenIds.map(e => `token_ids=${e}`).join('&');
            try {
                const httpResult = yield httpRequest.get(`https://api.opensea.io/api/v1/assets?order_direction=asc&asset_contract_address=${contractAddress}&limit=50&${tokenIdsQueries}`);
                if (httpResult && httpResult.data && httpResult.data.assets) {
                    return httpResult.data.assets.map(asset => ({
                        ADDRESS: contractAddress,
                        TOKEN_ID: asset.token_id,
                        NAME: asset.name,
                        TOKEN_METADATA: asset.token_metadata,
                        OWNER_ADDRESS: asset.image_url,
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
                    }));
                }
            }
            catch (e) {
                console.log(e);
                return [];
            }
            return [];
        });
    }
    getAsset(contractAddress, tokenId) {
        return __awaiter(this, void 0, void 0, function* () {
            const assets = yield this.getAssetsWithTokenIds(contractAddress, [tokenId]);
            if (assets.length > 0) {
                return assets[0];
            }
            return undefined;
        });
    }
    assetToDatabaseObject(asset) {
        return Object.assign(Object.assign({}, asset), {
            TRAITS: JSON.stringify(asset.TRAITS),
            LISTINGS: asset.LISTINGS ? JSON.stringify(asset.LISTINGS) : undefined
        });
    }
    upsertToDatabase(asset) {
        return __awaiter(this, void 0, void 0, function* () {
            const obejct = this.assetToDatabaseObject(asset);
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
            const values = [
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
            ];
            try {
                yield this.CP.writerQueryNotUpdated(`INSERT INTO ASSETS (${keys.join(",")}) VALUES (${new Array(values.length).fill("?").join(",")}) ON DUPLICATE KEY UPDATE ADDRESS=VALUES(ADDRESS), TOKEN_ID=VALUES(TOKEN_ID), NAME=VALUES(NAME), TOKEN_METADATA=VALUES(TOKEN_METADATA), OWNER_ADDRESS=VALUES(OWNER_ADDRESS), IMAGE_URL=VALUES(IMAGE_URL), IMAGE_PREVIEW_URL=VALUES(IMAGE_PREVIEW_URL), IMAGE_ORIGINAL_URL=VALUES(IMAGE_ORIGINAL_URL), ANIMATION_URL=VALUES(ANIMATION_URL), ANIMATION_ORIGINAL_URL=VALUES(ANIMATION_ORIGINAL_URL), TRAITS=VALUES(TRAITS), LAST_SALE_TX_HASH=VALUES(LAST_SALE_TX_HASH), LAST_SALE_PRICE=VALUES(LAST_SALE_PRICE), LAST_SALE_PAYMENT_TOKEN=VALUES(LAST_SALE_PAYMENT_TOKEN), LAST_SALE_PAYMENT_TOKEN_DECIMALS=VALUES(LAST_SALE_PAYMENT_TOKEN_DECIMALS), LAST_SALE_TIME=VALUES(LAST_SALE_TIME), LISTINGS=VALUES(LISTINGS)`, [...values]);
            }
            catch (e) {
                console.log(e);
            }
            return true;
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
                const startTime = new Date().getTime();
                if (this.jobId.tokenId == undefined) {
                    return {
                        success: false,
                        result: {
                            errorCode: 0,
                            errorMsg: "TokenId is undefined"
                        },
                    };
                }
                const asset = yield this.getAsset(this.jobId.contractAddress, this.jobId.tokenId);
                if (asset) {
                    const result = yield this.upsertToDatabase(asset);
                    if (result) {
                        const duration = new Date().getTime() - startTime;
                        if (duration < 1000) {
                            yield (0, waait_1.default)(1000 - duration);
                        }
                        return {
                            success: true,
                            result: {
                                asset: asset
                            },
                        };
                    }
                }
                return {
                    success: false,
                    result: {
                        errorCode: -2,
                        errorMsg: "Asset is undefined"
                    }
                };
            }
            catch (error) {
                if (error.response) {
                    // 요청이 이루어졌으며 서버가 2xx의 범위를 벗어나는 상태 코드로 응답했습니다.
                    // logger.info(error.response.data);
                    // logger.info(error.response.status);
                    // logger.info(error.response.headers);
                    Logger_1.default.error(`[MicroJob Error] Response Code: ${error.response.status} - ${error.response.data}`);
                    if (parseInt(error.response.status) == 429) {
                        Logger_1.default.error(`[MicroJob Error] Retry: ${retry}`);
                        yield (0, waait_1.default)(1500);
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
                    Logger_1.default.error(`[MicroJob Error] Request Error`);
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
                    Logger_1.default.error(`MicroJob Error] Undefined Error`);
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
exports.default = GetAsset;
