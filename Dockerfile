FROM golang:1.19-alpine

WORKDIR /app

# Download necessary Go modules
COPY go.mod ./
COPY go.sum ./
RUN go mod download

# Copy files
COPY . ./

# Necessary tools to build vaero (GCC, standard headers)
RUN apk add gcc libc-dev

# Bash shell for interactive use by users with /bin/bash
RUN apk add bash

# Install Python, Pip, and all dependencies
RUN apk add python3 py3-pip
RUN pip install -r requirements.txt

# Build vaero
RUN go install

# Expose port range for push sources
EXPOSE 8000-9000

# Execute
CMD ["vaero"]