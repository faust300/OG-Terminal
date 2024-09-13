import Axios from "axios";
import https from "https";
import Dotenv from "dotenv";
import { AbiItem } from "web3-utils";

Dotenv.config()

export const httpRequestCollection = Axios.create({
    timeout: 60000,
    httpsAgent: new https.Agent({ keepAlive: true }),
    headers: { 'Content-Type': 'application/json', 'X-API-KEY': process.env.OPENSEA_API_KEY as string }
});
export const httpRequestAssets = Axios.create({
    timeout: 60000,
    httpsAgent: new https.Agent({ keepAlive: true }),
    headers: { 'Content-Type': 'application/json', 'X-API-KEY': process.env.OPENSEA_API_KEY2 as string }
});

export const getNameABI = (): AbiItem[] => {
    return [
        {
            "inputs": [

            ],
            "name": "name",
            "outputs": [
                {
                    "internalType": "string",
                    "name": "",
                    "type": "string"
                }
            ],
            "stateMutability": "view",
            "type": "function"
        }
    ]
}
