#!/bin/bash
(
    docker compose -f docker-compose.clients.yaml up -d
) && echo "Clients started."
