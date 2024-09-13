import ConnectionPool from "libs-connection-pool";
import { Job } from "libs-job-manager";
import { logger } from "../..//utils/Logger";
import { Asset, CollectRequest, CollectResult } from "../../interfaces";
import SQL, { SQLStatement } from "sql-template-strings";

export default class CalcRarityJob extends Job<CollectRequest, CollectResult> {

    constructor(request: CollectRequest, private readonly cp: ConnectionPool) {
        super(request);
    }

    async calculateRarity(): Promise<boolean> {

        try {

            // Step1 : Get Assets From Database
            logger.info(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] Step1 : Get Assets From Database`);
            let assets = await this.cp.readerQuery<Asset[]>(SQL`SELECT ADDRESS, TOKEN_ID, TRAITS FROM ASSETS WHERE ADDRESS = ${this.request.contractAddress}`);

            // Step2 : Extract Traist from Assets
            logger.info(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] Step2 : Extract Traist from Assets (length:${assets.length})`);
            const traits = (assets as any[]).reduce((prev, cur) => {
                prev[cur.TOKEN_ID] = (cur.TRAITS as any[]).map(t => ({
                    traitType: t.trait_type,
                    value: t.value,
                }))
                return prev;
            }, {})

            // Step3 : Sum of attribute values ​​for each TraitType
            logger.info(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] Step3 : Sum of attribute values ​​for each TraitType`);
            const totalTraits = (Object.values(traits).flat() as any[]).reduce((prev, cur) => {
                if (prev[cur.traitType] == undefined) {
                    prev[cur.traitType] = {
                        count: 0,
                        values: {}
                    }
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
                return prev
            }, {})

            // Step4 : Calcutation score & ratio
            logger.info(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] Step4 : Calcutation score & ratio`);
            const traitsArray: any[] = [];
            Object.keys(totalTraits).forEach(type => {
                Object.keys(totalTraits[type].values).forEach(value => {
                    totalTraits[type].values[value].ratio = (totalTraits[type].values[value].count / assets.length * 100)
                    totalTraits[type].values[value].score = (1 / (totalTraits[type].values[value].count / assets.length))
                    traitsArray.push({
                        traitType: type,
                        value: value,
                        count: totalTraits[type].values[value].count,
                        typesCount: totalTraits[type].count,
                    })
                })
            })

            // Step5 : Mapping Score List to TokenId:Score-Object Map
            logger.info(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] Step5 : Mapping Score List to TokenId:Score-Object Map`);
            const assetsMap = (assets as any[]).reduce((prev, cur) => {
                const traits = (cur.TRAITS as any[]).map(t => ({
                    traitType: t.trait_type,
                    value: t.value,
                }))

                const score = traits.reduce((prev, cur) => {
                    prev += totalTraits[cur.traitType].values[cur.value].score
                    return prev
                }, 0)

                prev[cur.TOKEN_ID] = {
                    traits: traits,
                    score: score
                }
                return prev;
            }, {})

            // Step6 : Sorting and extract score
            logger.info(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] Step6 : Sorting and extract score`);
            let rankIds: any = {};
            Object.keys(assetsMap).reduce((prev, cur) => {
                prev[cur] = {
                    score: assetsMap[cur].score,
                    tokenId: cur
                };
                return prev
            }, rankIds)
            let rankIdsArray: any[] = Object.values(rankIds);
            const map = rankIdsArray.sort((a: any, b: any) => {
                if (a.score > b.score) {
                    return -1;
                } else if (a.score < b.score) {
                    return 1;
                } else {
                    return 0;
                }
            })

            // Step7 : Update Score and Rank to Database
            logger.info(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] Step7 : Update Score and Rank to Database`);
            const queries: SQLStatement = map.reduce((query, e, index) => {
                query.append(SQL`UPDATE ASSETS SET RARITY_RANK = ${index + 1} , RARITY_SCORE = ${e.score} WHERE ADDRESS = ${this.request.contractAddress} AND TOKEN_ID = ${e.tokenId};`)
                return query;
            }, SQL``)

            const traitsQueires: SQLStatement = traitsArray.reduce((query, e) => {
                query.append(SQL`INSERT INTO TRAITS (ADDRESS, TRAIT_TYPE, VALUE, COUNT, TYPES_COUNT) VALUES (${this.request.contractAddress}, ${e.traitType}, ${e.value}, ${e.count}, ${e.typesCount}) ON DUPLICATE KEY UPDATE ADDRESS=VALUES(ADDRESS), TRAIT_TYPE=VALUES(TRAIT_TYPE), VALUE=VALUES(VALUE), COUNT=VALUES(COUNT), TYPES_COUNT=VALUES(TYPES_COUNT);`)
                return query;
            }, SQL``)
            const connection = await this.cp.beginTransaction();
            try {
                await this.cp.query(connection, queries)
                await this.cp.query(connection, traitsQueires)
                await this.cp.commit(connection)
                connection.release();
            } catch (e: any) {
                connection.rollback(() => {
                    connection.release()
                    logger.error(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] ${(e as any).message}`);
                    logger.error(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] Rollback!`);
                })
            }

            return true;
        } catch (e) {
            logger.error(`[Terminal Asset Recoder] [CalcRarity] [${this.request.contractAddress}] ${(e as any).message}`);
        }
        return false;
    }

    async execute(): Promise<CollectResult> {
        const result = await this.calculateRarity()
        return {
            success: result,
            type: "Rarity",
            done: this.request.done
        }
    }

}