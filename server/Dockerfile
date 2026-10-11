FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production DATA_DIR=/data PUBLIC_DIR=/app PORT=8080
COPY server/package.json server/package-lock.json server/
RUN cd server && npm ci --omit=dev && npm cache clean --force
COPY server/src server/src
COPY client client
COPY docs docs
COPY examples examples
COPY MULTIPLAYER.md ./
RUN mkdir -p /data && chown node:node /data
USER node
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://127.0.0.1:8080/health || exit 1
CMD ["node", "--disable-warning=ExperimentalWarning", "server/src/index.js"]
