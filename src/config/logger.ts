import winston from 'winston';

const isProduction = process.env.NODE_ENV === 'production';

const transports: winston.transport[] = [
  new winston.transports.Console(),
];

// Only write to files in development
if (!isProduction) {
  try {
    transports.push(new winston.transports.File({ filename: 'logs/error.log', level: 'error' }));
    transports.push(new winston.transports.File({ filename: 'logs/combined.log' }));
  } catch {
    // ignore file transport errors
  }
}

export const logger = winston.createLogger({
  level: isProduction ? 'info' : 'debug',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    isProduction
      ? winston.format.json()
      : winston.format.combine(
          winston.format.colorize(),
          winston.format.printf(({ timestamp, level, message, ...meta }) => {
            const metaStr = Object.keys(meta).length ? JSON.stringify(meta) : '';
            return `${timestamp} [${level}]: ${message} ${metaStr}`;
          })
        )
  ),
  transports,
});
