import Winston, { Logger } from "winston"

const { combine, timestamp, printf } = Winston.format;
const logFormat = printf(info => {
    return `[${info.timestamp}] [${info.level}] : ${info.message}`;
});

export const logger: Logger = Winston.createLogger({
    format: combine(
        Winston.format.colorize(),
        timestamp({
            format: 'YYYY-MM-DD HH:mm:ss',
        }),
        logFormat,
    ),
    transports: [
    ]
}).add(new Winston.transports.Console({
    format: Winston.format.combine(
        Winston.format.colorize(),
        timestamp({
            format: 'YYYY-MM-DD HH:mm:ss',
        }),
        logFormat,
    )
}));