import { JobResult, JobReuqest } from "libs-job-manager";

export interface Contract {
    ADDRESS?: string,
    SCHEMA?: string,
    NETWORK?: string,
    CONTRACT_TYPE?: string,
    NAME?: string,
    NFT_VERSION?: string,
    IMAGE_URL?: string,
    SYMBOL?: string,
    TOTAL_SUPPLY?: string,
    DESCRIPTION?: string,
    EXTERNAL_LINK?: string,
    HAS_COLLECTION?: boolean,
    BANNER_IMAGE_URL?: string,
    COLLECTION_EXTERNAL_URL?: string,
    COLLECTION_IMAGE_URL?: string,
    FEATURED_IMAGE_URL?: string,
    LARGE_IMAGE_URL?: string,
    COLLECTION_NAME?: string,
    SLUG?: string,
    DISCORD_URL?: string,
    TELEGRAM_URL?: string,
    TWITTER_USERNAME?: string,
    INSTAGRAM_USERNAME?: string,
    WIKI_URL?: string,
    COLLECTED_ASSETS?: number,
    COLLECTING_ASSETS?: number,
}

export interface Asset {
    ADDRESS: string,
    TOKEN_ID: string,
    NAME: string,
    TOKEN_METADATA: string,
    OWNER_ADDRESS: string,
    IMAGE_URL: string,
    IMAGE_PREVIEW_URL: string,
    IMAGE_THUMBNAIL_URL: string,
    IMAGE_ORIGINAL_URL: string,
    ANIMATION_URL: string,
    ANIMATION_ORIGINAL_URL: string,
    TRAITS: any[],
    LAST_SALE_TX_HASH?: string,
    LAST_SALE_PRICE?: string,
    LAST_SALE_PAYMENT_TOKEN?: string,
    LAST_SALE_PAYMENT_TOKEN_DECIMALS?: number,
    LAST_SALE_TIME?: string,
    LISTINGS?: any[],
    CREATED_TIME?: string,
    UPDATED_TIME?: string,
}

export interface Trait{
    ADDRESS:string,
    TRAIT_TYPE:string,
    VALUE:string,
    COUNT:number,
    TYPES_COUNT:number
}

export interface Error {
    errorCode: number,
    errorMsg: string
}

export interface CollectResult extends JobResult {
    type: string,
    contractAddress?: string,
    contract?: Contract,
    requestAssets?: boolean,
    asset?: Asset,
    next?: string | undefined
    error?: Error,
    done: () => void
}

export interface CollectRequest extends JobReuqest {
    contractAddress: string,
    tokenId: string | undefined,
    next: string | undefined,
    cursorIndex: number | undefined,
    force?: boolean,
    done: () => void
}

export interface AssetsResult {
    next: string | undefined,
    previous: string | undefined,
    assets: Asset[]
}