import ConnectionPool from "libs-connection-pool";
import { Job } from "libs-job-manager";
import SQL from "sql-template-strings";
import Waait from "waait";
import Web3 from "web3";
import { CollectRequest, CollectResult, Contract } from "../../interfaces";
import { logger } from "../../utils/Logger";
import { getNameABI, httpRequestCollection } from "../../utils/Utils";

export default class GetCollectionJob extends Job<CollectRequest, CollectResult> {
    private readonly retryLimit: number = 5;
    private retryCount: number = 0;

    constructor(request: CollectRequest, private readonly cp: ConnectionPool, private readonly web3: Web3) {
        super(request)
    }

    async getContractInfoFromOpensea(contractAddress: string): Promise<Contract | undefined> {
        const httpResult = await httpRequestCollection.get(`https://api.opensea.io/api/v1/asset_contract/${contractAddress}`)
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
            }
        }
        return undefined
    }

    async getContractInfoFromDatabse(contractAddress: string): Promise<Contract | undefined> {
        return await this.cp.readerQuerySingle<Contract>("SELECT * FROM CONTRACTS WHERE `ADDRESS` = ?", [contractAddress])
    }

    async getContractInfoFromNode(contractAddress: string): Promise<Contract | undefined> {
        try {
            const name = await (new this.web3.eth.Contract(getNameABI(), contractAddress)).methods.name().call();
            return {
                ADDRESS: contractAddress,
                NAME: name,
                NETWORK: 'Ethereum',
            }
        } catch (e) {
            return undefined
        }
    }

    async checkRequireCollect(contractAddress: string): Promise<boolean> {
        const isExist = await this.cp.readerQuerySingle<Contract>(SQL`SELECT * FROM CONTRACTS WHERE CONTRACTS.ADDRESS = ${contractAddress} AND ( DATE_ADD(UPDATED_TIME, INTERVAL 6 HOUR) >= NOW() OR CONTRACTS.SLUG IS NULL );`);
        return isExist == undefined
    }

    async upsertContractInfo(contract: Contract): Promise<boolean> {
        try {
            await this.cp.writerQuery(SQL`
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
            `)
            return true;
        } catch (e) {
            console.log(e)
            return false;
        }
    }

    async getStats(slug: string): Promise<any> {
        try {
            const httpResult = await httpRequestCollection.get(`https://api.opensea.io/api/v1/collection/${slug}/stats`)
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
                }
            } else {
                console.log('you shall not pass')
            }
        } catch (e: any) {
            return undefined
        }
        return undefined
    }

    async checkRequireGetAssets(contract: Contract): Promise<boolean> {
        if (contract.SLUG) {
            const stats = await this.getStats(contract.SLUG)
            if(stats){
                try {
                    const queryResult = await this.cp.readerQuerySingle<Contract>(SQL`SELECT COLLECTED_ASSETS, COLLECTING_ASSETS FROM CONTRACTS WHERE CONTRACTS.ADDRESS = ${contract.ADDRESS}`)
                    
                    return queryResult != undefined
                        && (queryResult.COLLECTED_ASSETS != undefined && Boolean(!queryResult.COLLECTED_ASSETS!))
                        && (queryResult.COLLECTING_ASSETS != undefined && Boolean(!queryResult.COLLECTING_ASSETS!))
                        && stats.total_volume > 500;
                        
                } catch (error) {
                    console.log(error, 'Error')
                    console.log(contract.SLUG, contract.ADDRESS, stats, 'error')
                }
            }
        }
        return false;
    }

    async execute(): Promise<CollectResult> {
        logger.info(`[Terminal Asset Recoder] [Execute GetCollection] : ${this.request.contractAddress}`);
        const startTime = new Date().getTime()

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
            }
        }
        if (await this.checkRequireCollect(this.request.contractAddress) || this.request.force == true) {
            // Step 1. Get ContractInfo from Ethereum Node
            const contractInfoFromNode = await this.getContractInfoFromNode(this.request.contractAddress);
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
            const contractInfoFromDatabse = await this.getContractInfoFromDatabse(this.request.contractAddress);

            // Step 3. Get ContractInfo from Opensea
            try {
                const contractInfoFromOpensea = await this.getContractInfoFromOpensea(this.request.contractAddress);
                // Step 4. Merge ContractInfo
                const contractInfo = {
                    ...(contractInfoFromNode ? contractInfoFromNode : {}),
                    ...(contractInfoFromDatabse ? contractInfoFromDatabse : {}),
                    ...(contractInfoFromOpensea ? contractInfoFromOpensea : {})
                };
                if (contractInfo.ADDRESS && contractInfo.NAME) {
                    // Step 5. Upsert Contract Info
                    const result = await this.upsertContractInfo(contractInfo);
                    if (result) {
                        const checkRequireGetAssets = await this.checkRequireGetAssets(contractInfo);
                        // Check if 2 seconds have elapsed
                        const duration = new Date().getTime() - startTime;
                        if (duration < 2000) {
                            await Waait(2000 - duration)
                        }

                        return {
                            success: true,
                            type: "Collection",
                            done: this.request.done,
                            contract: contractInfo,
                            requestAssets: checkRequireGetAssets
                        }

                    } else {
                        return {
                            success: false,
                            type: "Collection",
                            done: this.request.done,
                            error: {
                                errorCode: 0,
                                errorMsg: "Failed Upsert for Database"
                            },
                        }
                    }
                } else {
                    return {
                        success: false,
                        type: "Collection",
                        done: this.request.done,
                        error: {
                            errorCode: 0,
                            errorMsg: "Collection lack of information"
                        },
                    }
                }

            } catch (e: any) {
                // Error: 429 is Too many request
                if(e.response){
                    if (parseInt(e.response.status) == 429) {
                        logger.error(`[Terminal Asset Recoder] [GetCollection] [${this.request.contractAddress}] 429 Error`);
                        await Waait(3500 * (Math.random() + 1));
                        this.retryCount++;
                        if (this.retryLimit > this.retryCount) {
                            return await this.execute();
                        } else {
                            return {
                                success: false,
                                type: "Collection",
                                done: this.request.done,
                                error: {
                                    errorCode: -3,
                                    errorMsg: "Request Error"
                                }
                            }
                        }
                    } else {
                        return {
                            success: false,
                            type: "Collection",
                            done: this.request.done,
                            error: {
                                errorCode: -3,
                                errorMsg: "Request Error"
                            }
                        }
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
                }
            }

        } else {
            return {
                success: false,
                type: "Collection",
                done: this.request.done,
                error: {
                    errorCode: 0,
                    errorMsg: "this contract none require collection"
                },
            }
        }
    }
}