# Use the official Node image as the base image
FROM node:18

# Set the working directory
WORKDIR /usr/src/app

# Copy Package files
COPY ts/feed-generator/package.json .
COPY ts/feed-generator/yarn.lock .

# Install the dependencies
RUN yarn install --frozen-lockfile

# Copy the source files to the container
COPY ts/feed-generator .

# Build the application
RUN yarn build

ENV PORT=8092

# Expose the port for the Feed Generator application
EXPOSE ${PORT}

# Start the application
CMD ["yarn", "start"]
