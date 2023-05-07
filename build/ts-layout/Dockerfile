# Use the official Rust image as the base image
FROM node:18

# Set the working directory
WORKDIR /usr/src/app

# Copy Package files
COPY ts/layout/package.json .
COPY ts/layout/yarn.lock .

# Install the dependencies
RUN yarn install --frozen-lockfile

# Copy the source files to the container
COPY ts/layout .

# Build the application
RUN yarn build

ENV PORT=8087

# Expose the port for the FastAPI application
EXPOSE ${PORT}

# Start the application
CMD ["yarn", "start"]
