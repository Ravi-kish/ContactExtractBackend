FROM node:20-alpine

WORKDIR /app

# Install dependencies first (node_modules not copied from host)
COPY package*.json ./
RUN npm ci

# Copy source (node_modules excluded via .dockerignore)
COPY . .

# Build TypeScript
RUN npm run build

RUN mkdir -p /app/uploads /app/logs

EXPOSE 3000

CMD ["node", "dist/server.js"]
