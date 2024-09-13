import { AMQModule } from "amqmodule";
import Dotenv from "dotenv";
import ConnectionPool from "libs-connection-pool";
import { JobManager } from "libs-job-manager";
import { Deduplication } from "./DeduplicationManager";
import { CollectRequest, CollectResult } from "./interfaces";
import GetAssetsJob from "./jobs/opensea/GetAssetsJob";
import GetCollectionJob from "./jobs/opensea/GetCollectionJob";
import CalcRarityJob from "./jobs/rarity/CalcRarityJob";

Dotenv.config();

export class CollectJobManager extends JobManager<CollectRequest, CollectResult, GetCollectionJob | GetAssetsJob | CalcRarityJob>{

    constructor(private readonly cp: ConnectionPool, private readonly mqInstance: AMQModule<any>) {
        super()
    }

    async onResult(jobResult: CollectResult): Promise<void> {
        if (jobResult.success) {
            if (jobResult.type == "Collection") {
                if (jobResult.requestAssets && jobResult.contract?.ADDRESS) {
                    // GetCollectionJob and request colelct Assets
                    this.mqInstance.publish(String(process.env.MQ_QUEUE_ASSETS), {
                        contractAddress: jobResult.contract?.ADDRESS
                    })
                }
            } else if (jobResult.type == "Assets") {
                if (jobResult.next && jobResult.contractAddress) {
                    const index = Number(await Deduplication.getCursor(jobResult.contractAddress))
                    this.mqInstance.publish(String(process.env.MQ_QUEUE_ASSETS), {
                        contractAddress: jobResult.contractAddress,
                        next: jobResult.next,
                        cursorIndex: index + 1
                    })
                } else if (jobResult.contractAddress) {
                    Deduplication.deleteCursor(jobResult.contractAddress)
                    this.mqInstance.publish(String(process.env.MQ_QUEUE_RARITY), {
                        contractAddress: jobResult.contractAddress,
                    })
                }
            }
        }
        jobResult.done()
    }
}

export default JobManager;
