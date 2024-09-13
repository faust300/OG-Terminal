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
const mysql2_1 = require("mysql2");
const Logger_1 = __importDefault(require("../../logger/Logger"));
const Job_1 = __importDefault(require("../../classes/Job"));
dotenv_1.default.config();
class CalcRarity extends Job_1.default {
    constructor(CP, request) {
        super(request);
        this.CP = CP;
    }
    calculateRarity() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                // Step1 : Get Assets From Database
                Logger_1.default.info(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] Step1 : Get Assets From Database`);
                let assets = yield this.CP.readerQuery("SELECT ADDRESS, TOKEN_ID, TRAITS FROM ASSETS WHERE ADDRESS = ?", [this.jobId.contractAddress]);
                // Step2 : Extract Traist from Assets
                Logger_1.default.info(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] Step2 : Extract Traist from Assets (length:${assets.length})`);
                const traits = assets.reduce((prev, cur) => {
                    prev[cur.TOKEN_ID] = cur.TRAITS.map(t => ({
                        traitType: t.trait_type,
                        value: t.value,
                    }));
                    return prev;
                }, {});
                // Step3 : Sum of attribute values ​​for each TraitType
                Logger_1.default.info(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] Step3 : Sum of attribute values ​​for each TraitType`);
                const totalTraits = Object.values(traits).flat().reduce((prev, cur) => {
                    if (prev[cur.traitType] == undefined) {
                        prev[cur.traitType] = {
                            count: 0,
                            values: {}
                        };
                    }
                    if (prev[cur.traitType].values[cur.value] == undefined) {
                        prev[cur.traitType].values[cur.value] = {
                            count: 0,
                            score: 0,
                            ratio: 0,
                        };
                    }
                    prev[cur.traitType].count = prev[cur.traitType].count + 1;
                    prev[cur.traitType].values[cur.value].count = prev[cur.traitType].values[cur.value].count + 1;
                    return prev;
                }, {});
                // Step4 : Calcutation score & ratio
                Logger_1.default.info(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] Step4 : Calcutation score & ratio`);
                const traitsArray = [];
                Object.keys(totalTraits).forEach(type => {
                    Object.keys(totalTraits[type].values).forEach(value => {
                        totalTraits[type].values[value].ratio = (totalTraits[type].values[value].count / assets.length * 100);
                        totalTraits[type].values[value].score = (1 / (totalTraits[type].values[value].count / assets.length));
                        traitsArray.push({
                            traitType: type,
                            value: value,
                            count: totalTraits[type].values[value].count,
                            typesCount: totalTraits[type].count,
                        });
                    });
                });
                // Step5 : Mapping Score List to TokenId:Score-Object Map
                Logger_1.default.info(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] Step5 : Mapping Score List to TokenId:Score-Object Map`);
                const assetsMap = assets.reduce((prev, cur) => {
                    const traits = cur.TRAITS.map(t => ({
                        traitType: t.trait_type,
                        value: t.value,
                    }));
                    const score = traits.reduce((prev, cur) => {
                        prev += totalTraits[cur.traitType].values[cur.value].score;
                        return prev;
                    }, 0);
                    prev[cur.TOKEN_ID] = {
                        traits: traits,
                        score: score
                    };
                    return prev;
                }, {});
                // Step6 : Sorting and extract score
                Logger_1.default.info(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] Step6 : Sorting and extract score`);
                let rankIds = {};
                Object.keys(assetsMap).reduce((prev, cur) => {
                    prev[cur] = {
                        score: assetsMap[cur].score,
                        tokenId: cur
                    };
                    return prev;
                }, rankIds);
                let rankIdsArray = Object.values(rankIds);
                const map = rankIdsArray.sort((a, b) => {
                    if (a.score > b.score) {
                        return -1;
                    }
                    else if (a.score < b.score) {
                        return 1;
                    }
                    else {
                        return 0;
                    }
                });
                // Step7 : Update Score and Rank to Database
                Logger_1.default.info(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] Step7 : Update Score and Rank to Database`);
                const queries = map.map((e, index) => (0, mysql2_1.format)(`UPDATE ASSETS SET RARITY_RANK = ? , RARITY_SCORE = ? WHERE ADDRESS = ? AND TOKEN_ID = ?`, [index + 1, e.score, this.jobId.contractAddress, e.tokenId]));
                const traitsQueires = traitsArray.map(e => (0, mysql2_1.format)(`INSERT INTO TRAITS (ADDRESS, TRAIT_TYPE, VALUE, COUNT, TYPES_COUNT) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ADDRESS=VALUES(ADDRESS), TRAIT_TYPE=VALUES(TRAIT_TYPE), VALUE=VALUES(VALUE), COUNT=VALUES(COUNT), TYPES_COUNT=VALUES(TYPES_COUNT)`, [this.jobId.contractAddress, e.traitType, e.value, e.count, e.typesCount]));
                const connection = yield this.CP.beginTransaction();
                try {
                    yield this.CP.query(connection, queries.join(";"), []);
                    yield this.CP.query(connection, traitsQueires.join(";"), []);
                    yield this.CP.commit(connection);
                    connection.release();
                }
                catch (e) {
                    connection.rollback(() => {
                        connection.release();
                        Logger_1.default.error(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] ${e.message}`);
                        Logger_1.default.error(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] Rollback!`);
                    });
                }
                return true;
            }
            catch (e) {
                Logger_1.default.error(`[Terminal Asset Recoder] [CalcRarity] [${this.jobId.contractAddress}] ${e.message}`);
            }
            return false;
        });
    }
    execute() {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield this.calculateRarity();
            return {
                success: result,
                result: {}
            };
        });
    }
}
exports.default = CalcRarity;
