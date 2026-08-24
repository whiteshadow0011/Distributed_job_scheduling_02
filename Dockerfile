FROM node:20-alpine

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci

COPY src/ ./src/
COPY test/ ./test/

EXPOSE 5000

CMD ["node", "src/api/server.js"]
