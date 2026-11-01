# NestJS Kafka Microservice Consumer

> TCP-তে Client ↔ Server সরাসরি কথা বলে। Kafka-তে মাঝখানে একটি **Broker** থাকে — **Producer** message পাঠায়, **Consumer** message নেয়।

---

## 📌 সংক্ষেপে (TL;DR)

| বিষয় | TCP | Kafka |
|---|---|---|
| Communication | Client ↔ Server সরাসরি | Producer → Broker → Consumer |
| Decorator | `@MessagePattern()` | `@EventPattern()` |
| Client method | `send()` | `emit()` |
| Model | Request–Response | Event-driven |
| App তৈরি | `NestFactory.create()` + `connectMicroservice()` | `NestFactory.createMicroservice()` |

---

## 📑 সূচিপত্র

1. [আগে মূল ধারণাগুলো](#1-আগে-মূল-ধারণাগুলো)
2. [পুরো কোড (main.ts)](#2-পুরো-কোড-maints)
3. [Import: Transport ও MicroserviceOptions](#3-import-transport-ও-microserviceoptions)
4. [createMicroservice()](#4-createmicroservice)
5. [transport: Transport.KAFKA](#5-transport-transportkafka)
6. [client Configuration](#6-client-configuration)
7. [consumer Configuration](#7-consumer-configuration)
8. [subscribe Configuration](#8-subscribe-configuration)
9. [app.listen()](#9-applisten)
10. [কোন Topic থেকে message আসবে?](#10-কোন-topic-থেকে-message-আসবে)
11. [দ্রুত রিভিশন](#11-দ্রুত-রিভিশন)

---

## 1. আগে মূল ধারণাগুলো

| শব্দ | মানে |
|---|---|
| **Kafka Broker** | Message গ্রহণ ও সংরক্ষণ করে |
| **Producer** | Kafka-তে message **পাঠায়** |
| **Consumer** | Kafka থেকে message **নেয়** |
| **Consumer Group** | একাধিক Consumer-কে একটি group-এর অধীনে চালায় |
| **Topic** | Message রাখার নির্দিষ্ট category / channel |

```
Producer ──► Kafka Broker (Topic: notifications) ──► Consumer
```

---

## 2. পুরো কোড (main.ts)

```ts
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
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
        },
      },
    },
  );

  await app.listen();
  console.log('Notification microservice is listening...');
}

bootstrap();
```

এখন প্রতিটি অংশ বুঝি 👇

---

## 3. Import: `Transport` ও `MicroserviceOptions`

```ts
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
```

### `Transport`
Microservice কোন **Transport** ব্যবহার করবে সেটা ঠিক করে।

```ts
Transport.TCP
Transport.KAFKA   // ✅ আমাদের ক্ষেত্রে
Transport.REDIS
Transport.RMQ
Transport.NATS
Transport.GRPC
```

### `MicroserviceOptions`
একটি **TypeScript Interface**। Configuration-এর Type যাচাই করে (Generic হিসেবে ব্যবহার হয়):

```ts
NestFactory.createMicroservice<MicroserviceOptions>()
```

---

## 4. `createMicroservice()`

```ts
const app = await NestFactory.createMicroservice<MicroserviceOptions>(
  AppModule,
  { transport: Transport.KAFKA, options: { /* ... */ } },
);
```

TCP Hybrid App-এ আমরা `NestFactory.create()` ব্যবহার করেছিলাম। এখানে আলাদা method:

| Method | কাজ |
|---|---|
| `NestFactory.create()` | **HTTP Application** তৈরি করে |
| `NestFactory.createMicroservice()` | **Standalone Microservice** তৈরি করে |

> 💡 এখানে কোনো REST API server তৈরি হচ্ছে না। এটি সরাসরি **Kafka Microservice** হিসেবে চলবে।

`AppModule` = Root Module (Controller, Provider, অন্যান্য Module এখানে যুক্ত থাকে)।

---

## 5. `transport: Transport.KAFKA`

```ts
transport: Transport.KAFKA,
```

NestJS-কে বলছি — TCP / Redis / RabbitMQ নয়, **Kafka Broker**-এর মাধ্যমে communicate করো।

আমাদের Broker চলছে: `localhost:9092`

---

## 6. `client` Configuration

```ts
client: {
  clientId: 'notification-consumer',
  brokers: ['localhost:9092'],
},
```

### `clientId`
Kafka-এর কাছে আমাদের Client-এর **unique নাম/পরিচয়**। যেকোনো নাম দেওয়া যায়:

```ts
clientId: 'order-service',
clientId: 'payment-service',
```

### `brokers`
Kafka Broker-এর **Address**।

```
localhost = Broker-এর Host
9092      = Broker-এর Port
```

একাধিক Broker থাকলে:

```ts
brokers: [
  'localhost:9092',
  'localhost:9093',
  'localhost:9094',
],
```

---

## 7. `consumer` Configuration

```ts
consumer: {
  groupId: 'notification-consumer-group',
},
```

`groupId` হলো **Consumer Group-এর পরিচয়**।

### Consumer Group কী?

ধরো, E-commerce app-এ order notification পাঠাতে ৩টি consumer চালালে:

```
notification-consumer-1
notification-consumer-2
notification-consumer-3
```

তিনটি একই group-এ: `notification-consumer-group`

Kafka এদের মধ্যে Topic-এর **Partition ভাগ করে** দেয়:

```
Topic: notifications
Consumer Group: notification-consumer-group

  Consumer 1  ──►  Partition 0
  Consumer 2  ──►  Partition 1
  Consumer 3  ──►  Partition 2
```

> ℹ️ এটি একটি উদাহরণ। বাস্তবে Partition ও Consumer সংখ্যা অনুযায়ী assignment হয়।

### ⚠️ গুরুত্বপূর্ণ

একই group-এর একাধিক consumer **একই Partition-এর একই message একসঙ্গে process করে না**। এতে কাজ ভাগ হয়ে যায়।

---

## 8. `subscribe` Configuration

```ts
subscribe: {
  fromBeginning: true,
}
```

Consumer Topic-এ subscribe করলে **কোথা থেকে পড়া শুরু করবে**, সেটা ঠিক করে।

### `fromBeginning: true`
আগে থেকে কোনো **Committed Offset না থাকলে**, Topic-এর সবচেয়ে পুরোনো available message থেকে পড়া শুরু করবে।

**উদাহরণ:** Topic-এ আগে থেকেই ৫টি message আছে:

```
Message 1 → Message 2 → Message 3 → Message 4 → Message 5
```

Consumer প্রথমবার subscribe করলে **Message 1 থেকে** পড়বে।

### `fromBeginning: false`
Committed Offset না থাকলে সাধারণত **নতুন আসা message** থেকে পড়া শুরু করবে।

### ⚠️ ভুল ধারণা এড়াও

`fromBeginning: true` মানে এই নয় যে **প্রতিবার restart-এ** আবার প্রথম থেকে পড়বে। আগে থেকে Committed Offset থাকলে Consumer সাধারণত **সেই Offset থেকেই** পরের message পড়ে।

| অবস্থা | `fromBeginning: true` | `fromBeginning: false` |
|---|---|---|
| Offset **নেই** | শুরু থেকে পড়ে | নতুন message থেকে পড়ে |
| Offset **আছে** | Offset থেকে পড়ে | Offset থেকে পড়ে |

---

## 9. `app.listen()`

```ts
await app.listen();
```

এটি Kafka Microservice **চালু** করে।

> 💡 HTTP server-এর মতো port listen করা নয়। বরং Kafka Broker-এর সঙ্গে **connection তৈরি** করে Consumer হিসেবে কাজ শুরু করে।

```
NestJS Kafka Consumer
        │
        ▼
Kafka Broker (localhost:9092)
        │
        ▼
Subscribe to Topic
        │
        ▼
Receive Messages ✅
```

আর এই line-টি:

```ts
console.log('Notification microservice is listening...');
```

শুধু Terminal-এ জানায় যে `app.listen()` সফল হয়েছে।

---

## 10. কোন Topic থেকে message আসবে?

`main.ts`-এ শুধু Kafka-র সঙ্গে connection ও Consumer configuration আছে। **কোন Topic** থেকে নেবে, সেটা এখনো ঠিক হয়নি।

এটা Controller-এ `@EventPattern()` দিয়ে ঠিক করতে হয়।

**`app.controller.ts`**

```ts
import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

@Controller()
export class AppController {

  @EventPattern('notifications')
  handleNotification(@Payload() data: any) {
    console.log('Received notification:', data);
  }
}
```

| Code | মানে |
|---|---|
| `@EventPattern('notifications')` | `notifications` Topic-এ event এলে নিচের method চলবে |
| `@Payload() data` | Kafka থেকে আসা message-এর payload গ্রহণ করে |

### পুরো Flow

```
┌──────────────────┐
│  Kafka Producer  │
└────────┬─────────┘
         │ message পাঠায়
         ▼
┌──────────────────────────────┐
│  Kafka Broker (localhost:9092)│
│  Topic: notifications        │
└────────┬─────────────────────┘
         │
         ▼
┌──────────────────────────────────┐
│  NestJS Kafka Consumer           │
│  Group: notification-consumer-group│
│                                  │
│  @EventPattern('notifications')  │
│  handleNotification(data)        │
│     └─ message process করে       │
└──────────────────────────────────┘
```

---

## 11. দ্রুত রিভিশন

| Code | কাজ |
|---|---|
| `Transport.KAFKA` | Kafka Transport নির্বাচন |
| `createMicroservice()` | Standalone Microservice তৈরি |
| `clientId` | Kafka Client-এর পরিচয় |
| `brokers` | Kafka Broker-এর Address |
| `groupId` | Consumer Group-এর পরিচয় |
| `fromBeginning: true` | Offset না থাকলে শুরু থেকে message পড়া |
| `app.listen()` | Kafka Microservice চালু করা |
| `@EventPattern()` | নির্দিষ্ট Topic-এর Event গ্রহণ |
| `@Payload()` | Message-এর Payload গ্রহণ |

### 🧠 TCP vs Kafka — শেখার সারকথা

```
TCP   →  @MessagePattern() + send()  =  Request–Response
Kafka →  @EventPattern()   + emit()  =  Event-driven
```

> Kafka-তে `@MessagePattern()` দিয়েও Request–Response করা যায়, তবে সাধারণ Event Consumer-এর জন্য `@EventPattern()` বেশি উপযুক্ত।

---

## 🚀 পরবর্তী ধাপ

**Kafka Producer** তৈরি করে `notifications` Topic-এ message পাঠাও, আর এই Consumer দিয়ে সেটা গ্রহণ করে practice করো। তাহলে Producer–Consumer Architecture পরিষ্কার হয়ে যাবে।
