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
exports.CollectJobManager = void 0;
const dotenv_1 = __importDefault(require("dotenv"));
const libs_job_manager_1 = require("libs-job-manager");
const DeduplicationManager_1 = require("./DeduplicationManager");
dotenv_1.default.config();
class CollectJobManager extends libs_job_manager_1.JobManager {
    constructor(cp, mqInstance) {
        super();
        this.cp = cp;
        this.mqInstance = mqInstance;
    }
    onResult(jobResult) {
        var _a, _b;
        return __awaiter(this, void 0, void 0, function* () {
            if (jobResult.success) {
                if (jobResult.type == "Collection") {
                    if (jobResult.requestAssets && ((_a = jobResult.contract) === null || _a === void 0 ? void 0 : _a.ADDRESS)) {
                        // GetCollectionJob and request colelct Assets
                        this.mqInstance.publish(String(process.env.MQ_QUEUE_ASSETS), {
                            contractAddress: (_b = jobResult.contract) === null || _b === void 0 ? void 0 : _b.ADDRESS
                        });
                    }
                }
                else if (jobResult.type == "Assets") {
                    if (jobResult.next && jobResult.contractAddress) {
                        const index = Number(yield DeduplicationManager_1.Deduplication.getCursor(jobResult.contractAddress));
                        this.mqInstance.publish(String(process.env.MQ_QUEUE_ASSETS), {
                            contractAddress: jobResult.contractAddress,
                            next: jobResult.next,
                            cursorIndex: index + 1
                        });
                    }
                    else if (jobResult.contractAddress) {
                        DeduplicationManager_1.Deduplication.deleteCursor(jobResult.contractAddress);
                        this.mqInstance.publish(String(process.env.MQ_QUEUE_RARITY), {
                            contractAddress: jobResult.contractAddress,
                        });
                    }
                }
            }
            jobResult.done();
        });
    }
}
exports.CollectJobManager = CollectJobManager;
exports.default = libs_job_manager_1.JobManager;
