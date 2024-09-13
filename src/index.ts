import { createJsonTypeInstance } from "amqmodule";
import { Provider, ProviderTypes } from "awsprovider";
import Dotenv from "dotenv";
import ConnectionPool from "libs-connection-pool";
import { Mode } from "libs-job-manager";
import Web3 from "web3";
import { CollectJobManager } from "./CollectJobManager";
import { Deduplication } from "./DeduplicationManager";
import GetAssetsJob from "./jobs/opensea/GetAssetsJob";
import GetCollectionJob from "./jobs/opensea/GetCollectionJob";
import CalcRarityJob from "./jobs/rarity/CalcRarityJob";
import { logger } from "./utils/Logger";
import { createClient } from "redis";


Dotenv.config();

const redisURL = `redis://@${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`;
export const redis = createClient({url: redisURL});
(async () => {
    try {
        await redis.connect();
    } catch (error) {
        logger.info(error)
    }

    const mqInstance = await createJsonTypeInstance({
        host: String(process.env.MQ_HOST),
        id: String(process.env.MQ_ID),
        pw: String(process.env.MQ_PW),
        port: parseInt(String(process.env.MQ_PORT))
    })
    mqInstance.setExchange(String(process.env.MQ_EXCHANGE))

    const cp = new ConnectionPool({
        host: String(process.env.MYSQL_HOST),
        writerHost: String(process.env.MYSQL_HOST),
        readerHost: String(process.env.MYSQL_RO_HOST),
        user: String(process.env.MYSQL_USER),
        password: String(process.env.MYSQL_PASSWORD),
        database: String(process.env.MYSQL_DATABASE),
    });

    const web3 = new Web3(Provider.from({
        type: ProviderTypes.HTTP,
        endpoint: String(process.env.HTTPS_END_POINT),
        accessKeyId: String(process.env.ACCESS_KEY_ID),
        secretAccessKey: String(process.env.SECRET_ACCESS_KEY),
    }))
    
    logger.info("[Terminal Asset Recoder] [Ready for Message Queue]");
    const jobManager = new CollectJobManager(cp, mqInstance).setMode(Mode.Async);

    mqInstance.consume(String(process.env.MQ_QUEUE_COLLECTION), (payload: any, done: () => void) => {
        logger.info("[Terminal Asset Recoder] [Consume GetCollectionJob]");
        jobManager.addJob(
            new GetCollectionJob({
                contractAddress: payload.contractAddress,
                tokenId: undefined,
                cursorIndex: undefined,
                next: undefined,
                force: payload.force? payload.force : false,
                done: done
            }, cp, web3)
        );
    }, 3)

    mqInstance.consume(String(process.env.MQ_QUEUE_ASSETS), async (payload: any, done: () => void) => {
        logger.info("[Terminal Asset Recoder] [Consume GetAssetsJob]");
        const cursorIndex = payload.cursorIndex ? payload.cursorIndex : 0
        const cursorResult = await Deduplication.checkCache(payload.contractAddress, cursorIndex)
        if (cursorResult) {
            logger.info(`[Terminal Asset Recoder] [GetAssets] [${payload.contractAddress}] Next:${payload.next} Cache:${cursorIndex}`);
            jobManager.addJob(
                new GetAssetsJob({
                    contractAddress: payload.contractAddress,
                    tokenId: undefined,
                    cursorIndex: cursorIndex,
                    next: payload.next,
                    force: payload.force,
                    done: done
                }, cp) 
            );
        } else {
            done()
        }
    }, 3)

    mqInstance.consume(String(process.env.MQ_QUEUE_RARITY), (payload: any, done: () => void) => {
        logger.info("[Terminal Asset Recoder] [Consume RarityJob]");
        jobManager.addJob(
            new CalcRarityJob({
                contractAddress: payload.contractAddress,
                tokenId: undefined,
                cursorIndex: undefined,
                next: undefined,
                done: done
            }, cp)
        );

    }, 150)

    // mqInstance.consume(String(process.env.MQ_QUEUE_ASSET), (payload: any, done: () => void) => {
    //     // Do not Has actions
    // }, 2)
})();