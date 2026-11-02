# NestJS Kafka Producer — Controller থেকে Event Publish

> আগে `ClientsModule.register()` দিয়ে Kafka Client Register করেছিলাম। এবার সেই Client-কে **Controller-এ Inject** করে `user_created` Event **Publish** করবো।

---

## 📌 সংক্ষেপে (TL;DR)

```
HTTP Request  ──►  AppController  ──►  kafkaClient.emit()  ──►  Kafka Topic: user_created
```

| Code | কাজ |
|---|---|
| `@Inject('KAFKA_SERVICE')` | Registered Kafka Client Inject করে |
| `onModuleInit()` + `connect()` | Kafka Broker-এর সঙ্গে Connection তৈরি করে |
| `emit('user_created', user)` | Topic-এ Event Publish করে |

---

## 📑 সূচিপত্র

1. [পুরো কোড](#1-পুরো-কোড)
2. [ClientKafka Import](#2-clientkafka-import)
3. [implements OnModuleInit](#3-implements-onmoduleinit)
4. [Constructor ও @Inject()](#4-constructor-ও-inject)
5. [onModuleInit()](#5-onmoduleinit)
6. [@Get('create-user')](#6-getcreate-user)
7. [User Object](#7-user-object)
8. [kafkaClient.emit()](#8-kafkaclientemit)
9. [return](#9-return)
10. [পুরো Architecture](#10-পুরো-architecture)
11. [কোড Improve করা](#11-কোড-improve-করা)
12. [Quick Revision](#12-quick-revision)

---

## 1. পুরো কোড

```ts
import { Controller, Get, Inject, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Controller()
export class AppController implements OnModuleInit {

  constructor(
    @Inject('KAFKA_SERVICE')
    private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @Get('create-user')
  async createUser() {
    const user = {
      id: 1,
      name: 'Wasim',
      email: 'wasim@example.com',
    };

    this.kafkaClient.emit('user_created', user);

    return {
      message: 'User Created Event Published!',
      user,
    };
  }
}
```

এবার এক এক করে বুঝি 👇

---

## 2. `ClientKafka` Import

```ts
import { ClientKafka } from '@nestjs/microservices';
```

`ClientKafka` হলো NestJS-এর **Kafka Client Class**। এর মাধ্যমে Application Kafka Broker-এর সঙ্গে communicate করে।

| Method | কাজ |
|---|---|
| `emit()` | Kafka-তে **Event Publish** করে |
| `send()` | Request পাঠিয়ে **Response গ্রহণ** করে |

এই উদাহরণে `emit()` ব্যবহার হয়েছে।

---

## 3. `implements OnModuleInit`

```ts
export class AppController implements OnModuleInit {
```

| অংশ | মানে |
|---|---|
| `implements` | TypeScript Keyword — Class-কে একটি Interface-এর Contract মানতে বলে |
| `OnModuleInit` | NestJS-এর **Lifecycle Hook Interface** |

`implements OnModuleInit` লিখলে Controller-এ **`onModuleInit()` method** থাকতেই হবে। NestJS Module Initialize হওয়ার পর সেটা চালায়।

---

## 4. Constructor ও `@Inject()`

```ts
constructor(
  @Inject('KAFKA_SERVICE')
  private readonly kafkaClient: ClientKafka,
) {}
```

এটি **Dependency Injection (DI)**-এর উদাহরণ।

### `@Inject('KAFKA_SERVICE')`

`AppModule`-এ আমরা এভাবে Register করেছিলাম:

```ts
ClientsModule.register([
  {
    name: 'KAFKA_SERVICE',
    transport: Transport.KAFKA,
    // ...
  },
])
```

`@Inject('KAFKA_SERVICE')` সেই Registered Client-কে Controller-এ এনে দেয়।

```
AppModule
   │  ClientsModule.register()
   ▼
KAFKA_SERVICE
   │  @Inject('KAFKA_SERVICE')
   ▼
AppController
```

### `private readonly kafkaClient: ClientKafka`

| অংশ | মানে |
|---|---|
| `private` | শুধু এই Class-এর ভেতরে ব্যবহার করা যাবে |
| `readonly` | একবার Assign হলে আর বদলানো যাবে না |
| `kafkaClient` | Variable-এর নাম |
| `ClientKafka` | Variable-এর Type |

---

## 5. `onModuleInit()`

```ts
async onModuleInit() {
  await this.kafkaClient.connect();
}
```

NestJS Controller Initialize করার সময় এই method চলে।

`connect()` Kafka Broker (`localhost:9092`)-এর সঙ্গে **Connection তৈরি** করে।

**কেন `async` / `await`?** Connection তৈরিতে সময় লাগে। `await` থাকায় Connection শেষ না হওয়া পর্যন্ত পরের কাজ অপেক্ষা করে।

```
NestJS Application Starts
          │
          ▼
Controller Initialized
          │
          ▼
onModuleInit()
          │
          ▼
kafkaClient.connect()
          │
          ▼
Kafka Broker Connected
          │
          ▼
Ready to Publish Events ✅
```

---

## 6. `@Get('create-user')`

```ts
@Get('create-user')
async createUser() {
```

এটি একটি **HTTP GET Route**:

```http
GET http://localhost:3000/create-user
```

এই URL-এ request গেলে `createUser()` চলবে।

> ⚠️ User **তৈরি** করার জন্য GET ব্যবহার করা ঠিক নয় — এটা নিয়ে [Section 11](#11-কোড-improve-করা)-এ বলা হয়েছে।

---

## 7. User Object

```ts
const user = {
  id: 1,
  name: 'Wasim',
  email: 'wasim@example.com',
};
```

এই Object-টাই Kafka-তে পাঠানো হবে।

> 💡 বাস্তব Application-এ এই Data সাধারণত **Database** থেকে তৈরি বা Retrieve করা হয়।

---

## 8. `kafkaClient.emit()`

```ts
this.kafkaClient.emit('user_created', user);
```

**সবচেয়ে গুরুত্বপূর্ণ লাইন।** Kafka Broker-এ Event **Publish** করে।

| Argument | মানে |
|---|---|
| `'user_created'` | Kafka **Topic**-এর নাম |
| `user` | Event-এর **Payload** (Data) |

`user_created` Topic-এ Subscribe করা যেকোনো Consumer এই Event পাবে।

### ⚠️ `emit()` সম্পর্কে মনে রাখো

- `emit()` একটি **Observable** return করে
- শুধু Publish করলে সাধারণত `await` লাগে না
- কিন্তু Publish **সফল হয়েছে কিনা নিশ্চিত হতে** বা **Error Handle** করতে চাইলে Observable-টি **subscribe** করতে হবে

---

## 9. `return`

```ts
return {
  message: 'User Created Event Published!',
  user,
};
```

HTTP Client-কে Response পাঠায়:

```json
{
  "message": "User Created Event Published!",
  "user": {
    "id": 1,
    "name": "Wasim",
    "email": "wasim@example.com"
  }
}
```

> 📝 **HTTP Response** আর **Kafka Event** দুটি **আলাদা** বিষয়।

---

## 10. পুরো Architecture

```
┌────────────────────────┐
│ HTTP Client / Browser  │
└───────────┬────────────┘
            │  GET /create-user
            ▼
┌────────────────────────┐
│ API Gateway (Producer) │
│ AppController          │
│ kafkaClient.emit()     │
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│ Kafka Broker           │
│ localhost:9092         │
│ Topic: user_created    │
└───────────┬────────────┘
            │
            ▼
┌──────────────────────────────────┐
│ Notification Service (Consumer)  │
│ @EventPattern('user_created')    │
│ handleUserCreated()              │
│   └─ Welcome Email / Notification│
└──────────────────────────────────┘
```

---

## 11. কোড Improve করা

বর্তমান কোডে দুটি সমস্যা আছে:

| # | সমস্যা | সমাধান |
|---|---|---|
| ১ | `emit()` করেই `return` হয়ে যায় — Event Publish শেষ হওয়ার **আগেই** HTTP Response চলে যেতে পারে | Observable-এর `complete` পর্যন্ত অপেক্ষা করা |
| ২ | Data **তৈরি** করতে `GET` ব্যবহার হচ্ছে | Industry Standard অনুযায়ী `POST` ব্যবহার করা |

### ✅ উন্নত কোড

```ts
import {
  Controller,
  Post,
  Inject,
  OnModuleInit,
} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Controller()
export class AppController implements OnModuleInit {

  constructor(
    @Inject('KAFKA_SERVICE')
    private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @Post('create-user')
  async createUser() {
    const user = {
      id: 1,
      name: 'Wasim',
      email: 'wasim@example.com',
    };

    await new Promise<void>((resolve, reject) => {
      this.kafkaClient.emit('user_created', user).subscribe({
        complete: resolve,
        error: reject,
      });
    });

    return {
      message: 'User Created Event Published!',
      user,
    };
  }
}
```

এখানে Event Publish Observable-এর `complete` না হওয়া পর্যন্ত অপেক্ষা করা হচ্ছে, আর Error হলে `reject` হবে।

> ⚠️ **মনে রাখো:** Event Publish সফল হওয়া মানেই এটা নয় যে Consumer Event **Process শেষ** করেছে।

---

## 12. Quick Revision

| Code | কাজ |
|---|---|
| `ClientKafka` | Kafka Client Class |
| `implements OnModuleInit` | Lifecycle Hook Interface Implement |
| `@Inject()` | Kafka Client Inject |
| `onModuleInit()` | Controller Initialize হলে Execute |
| `connect()` | Kafka Broker-এর সঙ্গে Connection |
| `@Get()` / `@Post()` | HTTP Endpoint তৈরি |
| `emit()` | Kafka Topic-এ Event Publish |
| `user` | Event Payload |
| `return` | HTTP Response |

### 🏗️ বর্তমান Kafka Learning Architecture

- **API Gateway** → Kafka Producer
- **Kafka Broker** → Message Management
- **Notification Service** → Kafka Consumer
- **`user_created`** → Kafka Topic

---

## 🚀 পরবর্তী ধাপ

Production-level Kafka Event Processing-এর জন্য শেখো:

- **Acknowledgment**
- **Offset Commit**
- **Retry**
- **Dead Letter Queue (DLQ)**
