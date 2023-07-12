#!/bin/bash
(
    docker compose -f docker-compose.clients.yaml up --build -d
) && echo "Clients started."
