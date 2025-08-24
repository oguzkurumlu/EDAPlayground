FROM node:20-slim
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev
COPY index.js ./
ENV NODE_ENV=production
EXPOSE 8080
CMD ["node", "index.js"]
