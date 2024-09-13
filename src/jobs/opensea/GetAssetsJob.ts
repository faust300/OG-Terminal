import Dotenv from "dotenv";
import ConnectionPool from "libs-connection-pool";
import { Job } from "libs-job-manager";
import Waait from "waait";
import { Asset, AssetsResult, CollectRequest, CollectResult } from "../../interfaces";
import { httpRequestAssets } from "../../utils/Utils";
import { logger } from "../../utils/Logger";
import SQL from "sql-template-strings";
import { OkPacket } from "mysql2";
import { Deduplication } from '../../DeduplicationManager';

Dotenv.config();

export default class GetAssetsJob extends Job<CollectRequest, CollectResult> {
    private readonly retryLimit: number = 5;
    private retryCount: number = 0;

    constructor(request: CollectRequest, private readonly cp: ConnectionPool) {
        super(request)
    }

    async getAssetsByCursor(contractAddress: string, cursor: string | undefined): Promise<AssetsResult> {
        let url: string = `https://api.opensea.io/api/v1/assets?order_direction=desc&limit=50&asset_contract_address=${contractAddress}${cursor ? `&cursor=${cursor}` : ``}`
        const httpResult = await httpRequestAssets.get(url)
        if (httpResult && httpResult.data) {
            const httpResultData = {
                next: httpResult.data.next,
                previous: httpResult.data.previous,
                assets: (httpResult.data.assets as any[]).map(asset => ({
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
                    LAST_SALE_TX_HASH: ( asset.last_sale && asset.last_sale.transaction ) ? asset.last_sale.transaction.transaction_hash : undefined,
                    LAST_SALE_PRICE: asset.last_sale ? asset.last_sale.total_price : undefined,
                    LAST_SALE_PAYMENT_TOKEN: (asset.last_sale && asset.last_sale.payment_token ) ? asset.last_sale.payment_token.address : undefined,
                    LAST_SALE_PAYMENT_TOKEN_DECIMALS: (asset.last_sale && asset.last_sale.payment_token ) ? asset.last_sale.payment_token.decimals : undefined,
                    LAST_SALE_TIME: ( asset.last_sale && asset.last_sale.transaction ) ? asset.last_sale.transaction.timestamp : undefined,
                }))
            }
            return httpResultData
        }

        return {
            next: undefined,
            previous: undefined,
            assets: []
        }
    }

    async upsertToDatabase(assets: Asset[]): Promise<boolean> {
        const queries = assets.reduce((query, asset) => {
            query.append(SQL`
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
            `)
            return query;
        }, SQL``)
        try {
            await this.cp.writerQuery(queries);
        } catch (e) {
            logger.error(`[Terminal Asset Recoder] [GetAssets] [${this.request.contractAddress}] ${(e as any).message}`);
            return false;
        }
        return true;
    }

    async getAssets(contractAddress: string, cursor: string | undefined): Promise<string | undefined> {
        const startTime = new Date().getTime()
        const assetsResult = await this.getAssetsByCursor(contractAddress, cursor)
        if (assetsResult.assets.length > 0) {
            await this.upsertToDatabase(assetsResult.assets);
        }
        const duration = new Date().getTime() - startTime;
        logger.info(`[Terminal Asset Recoder] [GetAssets] [${this.request.contractAddress}] Cursor:${cursor} / ${duration}ms`);
        if (duration < 1000) {
            await Waait(1000 - duration)
        }
        return assetsResult.next;
    }

    async execute(): Promise<CollectResult> {
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
            }
        }

        try {

            // Update CollectingAssets to true in Contracts
            await this.cp.writerQuery<OkPacket>(SQL`UPDATE CONTRACTS SET COLLECTING_ASSETS = ${true} WHERE ADDRESS = ${this.request.contractAddress};`)

            // TODO: Add Collecting Log
            if (this.request.cursorIndex == 0) {
                try {
                    await this.cp.writerQuery(SQL`INSERT COLLECT_ASSETS_LOG ( COLLECTING, REQUEST_TIME, CONTRACT_ADDRESS ) VALUES ( TRUE, CURRENT_TIMESTAMP(), ${this.request.contractAddress}) ON DUPLICATE KEY UPDATE REQUEST_TIME = CURRENT_TIMESTAMP(), COLLECTING = ${true}`)   
                } catch (error) {
                    console.log(error)                    
                }
            }
            const nextCusor = await this.getAssets(this.request.contractAddress, this.request.next);
            if (nextCusor == null || nextCusor == undefined) {
                const connection = await this.cp.beginTransaction()
                try {
                    await this.cp.query(connection, SQL`UPDATE CONTRACTS SET COLLECTED_ASSETS = ${true}, COLLECTING_ASSETS = ${false} WHERE ADDRESS = ${this.request.contractAddress};`)
                    await this.cp.query(connection, SQL`UPDATE COLLECT_ASSETS_LOG SET COLLECTED_TIME = CURRENT_TIMESTAMP(), COLLECTING = ${false} WHERE CONTRACT_ADDRESS = ${this.request.contractAddress};`)
                    await this.cp.commit(connection);
                    connection.release()
                    logger.info(`[Terminal Asset Recoder] [GetAssets] [${this.request.contractAddress}] Done`);
                } catch (e) {
                    connection.rollback(() => {
                        connection.release()
                    })
                }
            }

            if(nextCusor) {
                const updateNext = await Deduplication.updateNext(this.request.contractAddress, nextCusor)
            } else {
                const updateEnd = await Deduplication.updateEndPoint(this.request.contractAddress)
            }
            return {
                success: true,
                type: "Assets",
                done: this.request.done,
                contractAddress: this.request.contractAddress,
                next: nextCusor
            }

        } catch (e: any) {
            if (e && e.response && e.response.status && parseInt(e.response.status) == 429) {
                logger.error(`[Terminal Asset Recoder] [GetAssets] [${this.request.contractAddress}] 429 Error`);
                await Waait(3500 * (Math.random() + 1));
                this.retryCount++;
                if (this.retryLimit > this.retryCount) {
                    return await this.execute();
                } else {

                    Deduplication.deleteCursor(this.request.contractAddress!)
                    return {
                        success: false,
                        type: "Assets",
                        done: this.request.done,
                        error: {
                            errorCode: -3,
                            errorMsg: "Request Error"
                        }
                    }
                }
            }
            Deduplication.deleteCursor(this.request.contractAddress!)
            return {
                success: false,
                type: "Assets",
                done: this.request.done,
                error: {
                    errorCode: -3,
                    errorMsg: "Request Error"
                }
            }
        }
    }

}