## NestJS-Microservice-Kafka

#### Create Nx Workspace
```bash
npx create-nx-workspace@latest nest-kafka
```
```bash 
cd nest-kafka

```
---

#### Install Nx NestJS Plugin
```bash
npm i -d @nx/nest
```
---

#### Generate API Gateway Application
```bash
npx nx g @nx/nest:app api-gateway
```
---

#### Generate Notification Microservice
```bash
npx nx g @nx/nest:app notification-service
```
---

#### Install Kafka Dependencies
```bash
npm i @nestjs/microservices@^11 kafkajs
```
---

#### Setup Kafka with Docker
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
```yaml
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
docker compose up -d
```
> ei command deyar por jodi error ashe eta dekhun [Docker Compose Permission Denied Error (WSL Ubuntu)](https://github.com/Omarmdwasimuddin/Docker-Compose-Permission-Denied-Error-WSL-Ubuntu-)
---

####
```bash
docker logs -f kafka
```
---

#### `notification-service/src/main.ts`
```ts
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app/app.module';
import { MicroserviceOptions, Transport } from "@nestjs/microservices";

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'notification-consumer',
        brokers: ['localhost:9092'],
      },
      consumer: {
        groupId: 'notification-consumer-group',
      },
      subscribe: {
        fromBeginning: true,
      }
    }
  });
  await app.listen();
  console.log('Notification microservice is listening...');
}

bootstrap();
```
---

#### `notification-service/src/app/app.controller.ts`
```ts
import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

@Controller()
export class AppController {
  @EventPattern('user_created')
  handleUserCreated(@Payload() data: any) {
    console.log('Event received!');
    console.log(data);
  }
}
```
---

#### `api-gateway/src/main.ts`
```ts
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app/app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  await app.listen(3000);
  console.log('API Gateway is Running!');
}

bootstrap();
```
---

#### `api-gateway/src/app/app.module.ts`
```ts
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { ClientsModule, Transport } from '@nestjs/microservices';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'api-gateway',
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: 'api-gateway-group',
          }
        }
      }
    ])
  ],
  controllers: [AppController],
})
export class AppModule {}
```
---

#### `api-gateway/src/app/app.controller.ts`
```ts
import { Controller, Get, Inject, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Controller()
export class AppController implements OnModuleInit {
  constructor(
    @Inject('KAFKA_SERVICE') private readonly kafkaClient: ClientKafka
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @Get('create-user')
  async createUser() {
    const user = {
      id: 1,
      name: 'Wasim',
      email: 'wasim@example.com'
    };
    this.kafkaClient.emit('user_created', user);
    return { message: 'User Created Event Published!', user };
  }
  
}
```
---

####
```bash
npx nx serve notification-service
```
```bash
npx nx serve api-gateway
```
---

####
```bash
http://localhost:3000/create-user
```
<img width="386" height="219" alt="image" src="https://github.com/user-attachments/assets/638d3934-c6b5-406c-8aa5-0c6ecf888cf5" />

---
