FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install
RUN npm install -g pm2

# Copy the source code
COPY . .

# Start the sync engine using pm2-runtime for Docker
CMD ["pm2-runtime", "sync.mjs", "--name", "worldcup-live-sync"]
