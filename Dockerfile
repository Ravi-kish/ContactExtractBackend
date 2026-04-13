FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

RUN mkdir -p /app/uploads /app/logs

EXPOSE 3000

CMD ["npx", "ts-node", "--transpile-only", "src/server.ts"]
