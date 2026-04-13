FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

RUN npm run build

# Verify dist was created
RUN ls -la dist/ && echo "Build successful"

RUN mkdir -p /app/uploads /app/logs

EXPOSE 3000

CMD ["node", "dist/server.js"]
