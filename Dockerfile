FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

RUN npm run build

RUN mkdir -p /app/uploads /app/logs

EXPOSE 3000

CMD ["node", "dist/server.js"]
