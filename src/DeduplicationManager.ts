import { createClient } from "redis";
import moment from 'moment'
import { logger } from './utils/Logger';
import { redis } from './index';

class DeduplicationManager {
    private txHashs: Map<string, string> = new Map<string, string>();
    private cursors: Map<string, number> = new Map<string, number>();
    
    async getCache(key: string) {
        const contract = await redis.hGetAll(key);
        return contract;
    }

    async checkCache(key: string, cursorIndex: number):Promise<boolean> {
        const now = new Date().getTime();
        const nowUnixTime = moment(now).unix();
        const contract = await redis.hGetAll(key);
        const contractBoolean = contract.next;
        const endPointBoolean = !Number(contract.endPoint) || Number(contract.endPoint) === 0 ? false : true;
        let returnBoolean = false
        if(!endPointBoolean){
            if(contractBoolean){
                if(Number(contract.cursorIndex) < cursorIndex){
                    redis.hSet(key, {
                        cursorIndex: cursorIndex
                    })
                }
                returnBoolean = true
            } else {
                redis.hSet(key, {
                    next: 'undefined',
                    cursorIndex: cursorIndex,
                    timeStamp: String(nowUnixTime),
                    endPoint: 0
                })
                returnBoolean = true
            }
        }

        return returnBoolean
    }

    async deleteCursor(key: string) {
        await redis.del(key)
    }

    async updateEndPoint(key: string) {
        redis.hSet(key, {
            endPoint: 1
        })
    }

    async getCursor(key: string):Promise<number> {
        const contract = await redis.hGetAll(key)
        return Number(contract.cursorIndex);
    }

    async upCursor(key: string) {
        const contract = await redis.hGetAll(key)
        const value = Number(contract.cursorIndex)
        if (value) {
            redis.hSet(key, {
                cursorIndex: value + 1,
            })
        }
    }

    async updateNext(key: string, next: string) {
        try {
            redis.hSet(key, {
                next: next
            })
        } catch (error) {
            logger.info(error)
        }
    }
}

export const Deduplication: DeduplicationManager = new DeduplicationManager()