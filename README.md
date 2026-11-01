## NestJS-Microservice-Kafka

####
```bash
npx create-nx-workspace@latest nest-kafka
```
```bash
cd nest-kafka
```
---

####
```bash
npm i -d @nx/nest
```
---

####
```bash
npx nx g @nx/nest:app api-gateway
```
---

####
```bash
npx nx g @nx/nest:app notification-service
```
---

####
```bash
npm i @nestjs/microservices@^11 kafkajs
```
---

####
```bash
wsl -d Ubuntu
```
```bash
docker --version
```
```bash
docker compose version
```
```bash
nano docker-compose.yml
```
```ts
services:
  kafka:
    image: apache/kafka:3.9.0

    container_name: kafka

    ports:
      - "9092:9092"

    environment:
      KAFKA_NODE_ID: 1

      KAFKA_PROCESS_ROLES: broker,controller

      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093

      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
```
> code paste koro
> Ctrl+s click koro

<img width="805" height="851" alt="image" src="https://github.com/user-attachments/assets/b2cc140d-8b61-44ba-ba2c-630ab5bcabf2" />

> Ctrl+x click koro
```bash

```
---

####
```bash

```
---

####
```bash

```
---
