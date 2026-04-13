FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

# Compile TypeScript to JavaScript
RUN npm run build

RUN mkdir -p /app/uploads /app/logs

EXPOSE 3000

# Run compiled JS — no ts-node permission issues
CMD ["node", "dist/server.js"]
