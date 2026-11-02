# NestJS Kafka Client (Producer) — `ClientsModule.register()`

> আগে আমরা Kafka **Consumer** বানিয়েছি। এবার বানাচ্ছি Kafka **Client**, যা **Producer** হিসেবে কাজ করে — অর্থাৎ API Gateway থেকে Kafka-তে message/event পাঠাবে।

---

## 📌 সংক্ষেপে (TL;DR)

```
API Gateway (Client / Producer)
      │  emit('user_created', data)
      ▼
Kafka Broker  (Topic: user_created)
      │
      ▼
Notification Service (Consumer)
      @EventPattern('user_created')
```

| বিষয় | Consumer (আগে) | Client / Producer (এখন) |
|---|---|---|
| তৈরির উপায় | `NestFactory.createMicroservice()` | `ClientsModule.register()` |
| কাজ | Message **নেয়** | Message **পাঠায়** |
| Decorator / Type | `@EventPattern()` | `@Inject()` + `ClientKafka` |

---

## 📑 সূচিপত্র

1. [পুরো কোড (AppModule)](#1-পুরো-কোড-appmodule)
2. [Import: ClientsModule ও Transport](#2-import-clientsmodule-ও-transport)
3. [ClientsModule.register()](#3-clientsmoduleregister)
4. [name: 'KAFKA_SERVICE'](#4-name-kafka_service)
5. [transport: Transport.KAFKA](#5-transport-transportkafka)
6. [options Configuration](#6-options-configuration)
7. [পুরো Architecture](#7-পুরো-architecture)
8. [Controller থেকে Client ব্যবহার](#8-controller-থেকে-client-ব্যবহার)
9. [Production-এর গুরুত্বপূর্ণ বিষয়](#9-production-এর-গুরুত্বপূর্ণ-বিষয়)
10. [Quick Revision](#10-quick-revision)

---

## 1. পুরো কোড (AppModule)

```ts
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { AppController } from './app.controller';

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
          },
        },
      },
    ]),
  ],
  controllers: [AppController],
})
export class AppModule {}
```

এবার প্রতিটি অংশ বুঝি 👇

---

## 2. Import: `ClientsModule` ও `Transport`

```ts
import { ClientsModule, Transport } from '@nestjs/microservices';
```

### `ClientsModule`
অন্য Microservice বা Message Broker-এর সঙ্গে যোগাযোগের জন্য **Client তৈরি ও Register** করে।

যেমন: Kafka Client, TCP Client, Redis Client, RabbitMQ Client। আমাদের ক্ষেত্রে **Kafka Client**।

### `Transport`
Client কোন **Transport Protocol** ব্যবহার করবে:

```ts
Transport.TCP
Transport.KAFKA   // ✅ আমাদের ক্ষেত্রে
Transport.REDIS
Transport.RMQ
```

---

## 3. `ClientsModule.register()`

```ts
ClientsModule.register([
  {
    name: 'KAFKA_SERVICE',
    transport: Transport.KAFKA,
    options: { /* Configuration */ },
  },
])
```

এটাই সবচেয়ে গুরুত্বপূর্ণ অংশ। NestJS Application-এর ভেতরে একটি **Microservice Client Register** করে।

```
NestJS API Gateway
       │
       ▼
ClientsModule.register()
       │
       ▼
Kafka Client Created
       │
       ▼
Kafka Broker Connection
```

এরপর এই Client-কে যেকোনো Controller / Service-এ **Inject** করে ব্যবহার করা যায়।

---

## 4. `name: 'KAFKA_SERVICE'`

```ts
name: 'KAFKA_SERVICE',
```

এটি Client-এর **Injection Token (Identifier)**। এর মাধ্যমে NestJS বোঝে কোন Client Inject করতে হবে।

```ts
@Inject('KAFKA_SERVICE')
private kafkaClient: ClientKafka
```

> ⚠️ **নিয়ম:** `@Inject()`-এ দেওয়া নাম অবশ্যই `register()`-এর `name`-এর সঙ্গে **হুবহু মিলতে হবে**।

নাম বদলানো যায়:

```ts
name: 'NOTIFICATION_SERVICE'
// Inject করার সময়ও একই নাম
```

---

## 5. `transport: Transport.KAFKA`

```ts
transport: Transport.KAFKA,
```

এই Client **Kafka** ব্যবহার করে communicate করবে। অর্থাৎ API Gateway সরাসরি HTTP request না পাঠিয়ে **Kafka Broker-এর মাধ্যমে** অন্য Microservice-এর কাছে Event/Message পাঠাবে।

---

## 6. `options` Configuration

```ts
options: {
  client: {
    clientId: 'api-gateway',
    brokers: ['localhost:9092'],
  },
  consumer: {
    groupId: 'api-gateway-group',
  },
}
```

### ১) `clientId`
Kafka Client-এর **পরিচয়**। Broker-এর কাছে এই Client `api-gateway` নামে কাজ করবে।

### ২) `brokers`
Kafka Broker-এর **Address**।

```
Host: localhost
Port: 9092
```

একাধিক Broker থাকলে:

```ts
brokers: [
  'localhost:9092',
  'localhost:9093',
  'localhost:9094',
]
```

### ৩) `consumer.groupId`
Kafka **Consumer Group-এর Identifier**।

**প্রশ্ন:** Client তো Producer, তাহলে Consumer config কেন?

**উত্তর:** NestJS-এর `ClientKafka` শুধু Producer নয়। `send()` ব্যবহার করলে **Response নেওয়ার জন্য** Consumer হিসেবেও কাজ করে।

| Method | কাজ |
|---|---|
| `emit()` | Event **Publish** করে |
| `send()` | Request পাঠায় এবং **Response গ্রহণ** করে |

তাই `send()` ব্যবহার করলে Consumer Group গুরুত্বপূর্ণ।

---

## 7. পুরো Architecture

```
┌──────────────────────────────┐
│  API Gateway                 │
│  NestJS Application          │
│  Client ID: api-gateway      │
│  ClientsModule.register()    │
└──────────────┬───────────────┘
               │  emit('user_created', data)
               ▼
┌──────────────────────────────┐
│  Kafka Broker                │
│  localhost:9092              │
│  Topic: user_created         │
└──────────────┬───────────────┘
               │  Event Delivery
               ▼
┌──────────────────────────────┐
│  Notification Service        │
│  Kafka Consumer              │
│  @EventPattern('user_created')│
└──────────────────────────────┘
```

**ধাপে ধাপে:**

1. API Gateway Kafka Client তৈরি করে
2. API Gateway Kafka Broker-এ Event পাঠায়
3. Kafka Broker Event গ্রহণ করে
4. Notification Service Event গ্রহণ করে
5. Notification Service প্রয়োজনীয় কাজ করে

---

## 8. Controller থেকে Client ব্যবহার

শুধু Register করলেই হবে না — Client-কে **Inject করে ব্যবহার** করতে হবে।

**`app.controller.ts`**

```ts
import { Controller, Inject, Post } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Controller()
export class AppController {

  constructor(
    @Inject('KAFKA_SERVICE')
    private readonly kafkaClient: ClientKafka,
  ) {}

  @Post('create-user')
  createUser() {
    this.kafkaClient.emit('user_created', {
      id: 101,
      name: 'Wasim',
      email: 'wasim@example.com',
    });

    return {
      message: 'User created event published',
    };
  }
}
```

| Code | মানে |
|---|---|
| `@Inject('KAFKA_SERVICE')` | AppModule-এ Register করা Kafka Client Inject করে |
| `ClientKafka` | Kafka Client-এর Type |
| `emit('user_created', {...})` | `user_created` Topic-এ Payload Publish করে |

**API Request:**

```http
POST http://localhost:3000/create-user
```

এটি পাঠালে API Gateway Kafka-তে `user_created` Event Publish করবে।

---

## 9. Production-এর গুরুত্বপূর্ণ বিষয়

| ব্যবহার | `consumer.groupId` লাগে? | অতিরিক্ত সেটআপ |
|---|---|---|
| শুধু `emit()` | প্রয়োজন নেই | লাগে না |
| `send()` (Request–Response) | গুরুত্বপূর্ণ | লাগে (নিচে দেখো) |

`send()` ব্যবহার করলে **Reply Topic**-এ subscribe করতে হয় এবং Client **connect** করে নিতে হয়:

```ts
async onModuleInit() {
  this.kafkaClient.subscribeToResponseOf('get_user');
  await this.kafkaClient.connect();
}
```

> 💡 এটি শুধু **Request–Response**-এর জন্য প্রাসঙ্গিক। শুধু Event Publish করলে এই অতিরিক্ত সেটআপ লাগে না।

---

## 10. Quick Revision

| Code | কাজ |
|---|---|
| `ClientsModule` | Microservice Client তৈরি ও Register |
| `register()` | Client Configuration Register |
| `name` | Client-এর Injection Token |
| `Transport.KAFKA` | Kafka Transport নির্বাচন |
| `clientId` | Kafka Client-এর পরিচয় |
| `brokers` | Kafka Broker-এর Address |
| `groupId` | Consumer Group-এর পরিচয় |
| `ClientKafka` | Kafka Client-এর Type |
| `@Inject()` | Registered Client Inject |
| `emit()` | Event Publish |
| `send()` | Request–Response Communication |

### 🧠 একটি জরুরি পার্থক্য

```
createMicroservice()       →  Kafka Consumer Application তৈরি করে
ClientsModule.register()   →  অন্য Microservice-এর সঙ্গে যোগাযোগের Kafka Client তৈরি করে
```

এই পার্থক্য বুঝলে NestJS Kafka Architecture-এর **Producer** ও **Consumer** অংশ অনেক সহজ হয়ে যাবে। 🚀
