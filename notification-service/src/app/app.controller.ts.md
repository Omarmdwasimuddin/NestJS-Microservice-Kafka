# NestJS Kafka `@EventPattern()` — Event Consumer

> আগে শিখেছি `@MessagePattern()` (Request–Response)। এবার শিখছি `@EventPattern()` (Event-driven)। দুটোর পার্থক্য বোঝা Kafka Microservices-এর জন্য খুব গুরুত্বপূর্ণ।

---

## 📌 সংক্ষেপে (TL;DR)

```
Producer:  client.emit('user_created', data)
              │
              ▼
Kafka Topic: user_created
              │
              ▼
Consumer:  @EventPattern('user_created')
           handleUserCreated(@Payload() data) { ... }
```

| Code | কাজ |
|---|---|
| `emit()` | Event **Publish** করে |
| `@EventPattern()` | কোন Event/Topic **গ্রহণ** করবে |
| `@Payload()` | Event-এর **Data** গ্রহণ করে |
| `handleUserCreated()` | Event পাওয়ার পর **কী কাজ** করবে |

---

## 📑 সূচিপত্র

1. [পুরো কোড](#1-পুরো-কোড)
2. [Import: EventPattern ও Payload](#2-import-eventpattern-ও-payload)
3. [@EventPattern('user_created')](#3-eventpatternuser_created)
4. [handleUserCreated()](#4-handleusercreated)
5. [console.log() দুটি](#5-consolelog-দুটি)
6. [পুরো Communication Flow](#6-পুরো-communication-flow)
7. [@MessagePattern() vs @EventPattern()](#7-messagepattern-vs-eventpattern)
8. [Producer থেকে Event পাঠানো](#8-producer-থেকে-event-পাঠানো)
9. [পরবর্তী ধাপ](#9-পরবর্তী-ধাপ)

---

## 1. পুরো কোড

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

## 2. Import: `EventPattern` ও `Payload`

```ts
import { EventPattern, Payload } from '@nestjs/microservices';
```

### `EventPattern`
একটি **Decorator**। নির্দিষ্ট Event-এর জন্য **Handler Method** ঠিক করে।

```ts
@EventPattern('user_created')
```

অর্থ: Kafka-র `user_created` Topic থেকে Event এলে এই method চলবে।

### `Payload`
একটি **Parameter Decorator**। Event-এর সঙ্গে আসা **মূল Data** গ্রহণ করে।

Producer যদি এটা পাঠায়:

```json
{
  "id": 101,
  "name": "Wasim",
  "email": "wasim@example.com"
}
```

Consumer সেটা `@Payload()` দিয়ে পাবে।

---

## 3. `@EventPattern('user_created')`

```ts
@EventPattern('user_created')
```

এখানে `user_created` হলো **Kafka Topic-এর নাম**।

Consumer ওই Topic-এ subscribe করে। সেখানে Event এলে NestJS এই method চালায়।

### 🛒 বাস্তব উদাহরণ

E-commerce app-এ একজন নতুন User Registration করলো:

```
User Service  ──►  Event: user_created  ──►  Notification Service
                                              └─ Welcome Email পাঠায়
```

> ⚠️ **গুরুত্বপূর্ণ:** `@EventPattern()`-এ আমরা "Event-এর নাম" নয়, **Kafka Topic-এর নাম** দিচ্ছি। Kafka Transport-এ এই Pattern-টি Topic হিসেবেই ব্যবহার হয়।

---

## 4. `handleUserCreated()`

```ts
handleUserCreated(@Payload() data: any) {
```

এটি একটি **Event Handler Method**।

| অংশ | মানে |
|---|---|
| `handleUserCreated` | Method-এর নাম |
| `@Payload()` | Event-এর মূল Data গ্রহণ করে |
| `data: any` | Data-র Type নির্দিষ্ট করা হয়নি |

`data`-র ভেতরে থাকবে:

```ts
{
  id: 101,
  name: 'Wasim',
  email: 'wasim@example.com'
}
```

### Data ব্যবহার করা

```ts
handleUserCreated(@Payload() data: any) {
  console.log(data.name);
  console.log(data.email);
}
```

**Output:**

```
Wasim
wasim@example.com
```

> ⚠️ **Production tip:** `data: any`-র বদলে নির্দিষ্ট **Type বা DTO** ব্যবহার করা ভালো।

---

## 5. `console.log()` দুটি

```ts
console.log('Event received!');   // Event এসেছে কিনা বোঝার জন্য
console.log(data);                // পুরো Payload দেখার জন্য
```

**Output:**

```
Event received!
{ id: 101, name: 'Wasim', email: 'wasim@example.com' }
```

দুটোই **Debug**-এর জন্য।

---

## 6. পুরো Communication Flow

```
┌────────────────────────┐
│  User Service          │
│  নতুন User Registration│
└───────────┬────────────┘
            │  Publish Event
            ▼
┌────────────────────────┐
│  Kafka Broker          │
│  Topic: user_created   │
└───────────┬────────────┘
            │  Event Delivery
            ▼
┌────────────────────────────────┐
│  Notification Service          │
│                                │
│  @EventPattern('user_created') │
│  handleUserCreated()           │
│     └─ Welcome Email পাঠায়     │
└────────────────────────────────┘
```

### ✅ বড় সুবিধা

User Service-কে Notification Service-এর **Address জানতে হয় না**। শুধু Kafka-তে Event Publish করে দিলেই হয়। Notification Service নিজের দায়িত্ব অনুযায়ী Event নিয়ে নেয়।

> এটাই **Event-Driven Architecture**-এর একটি প্রধান বৈশিষ্ট্য — Service গুলো একে অপরের উপর কম নির্ভরশীল (loosely coupled)।

---

## 7. `@MessagePattern()` vs `@EventPattern()`

| বিষয় | `@MessagePattern()` | `@EventPattern()` |
|---|---|---|
| Communication | Request–Response | Event-driven |
| Client Method | `send()` | `emit()` |
| Response | Client **Response পায়** | Response-র জন্য **অপেক্ষা করে না** |
| ব্যবহার | Data চাওয়া / Operation-এর ফলাফল পাওয়া | Event Notification |
| উদাহরণ | `get_user` | `user_created` |

### Request–Response উদাহরণ

```ts
@MessagePattern({ cmd: 'get_user' })
getUser(@Payload() id: number) {
  return this.userService.findOne(id);
}
```

Client User-র Data **চাইছে** এবং **Response আশা করছে**।

### Event উদাহরণ

```ts
@EventPattern('user_created')
handleUserCreated(@Payload() data: any) {
  console.log('New user:', data);
}
```

শুধু **জানানো হচ্ছে** যে "User তৈরি হয়েছে"। কেউ উত্তরের অপেক্ষায় নেই।

### 🧠 সহজ মনে রাখা

```
প্রশ্ন করছি, উত্তর চাই   →  @MessagePattern + send()
খবর দিচ্ছি, উত্তর লাগে না →  @EventPattern   + emit()
```

---

## 8. Producer থেকে Event পাঠানো

Consumer কাজ করার জন্য:

1. Consumer-কে `user_created` Topic-এ **subscribe** করতে হবে (`@EventPattern('user_created')` এটা ঠিক করে)
2. Producer থেকে Event **পাঠাতে** হবে

**Producer-এর কোড:**

```ts
client.emit('user_created', {
  id: 101,
  name: 'Wasim',
  email: 'wasim@example.com',
});
```

`emit()` Event **Publish** করে, আর Consumer-এর `handleUserCreated()` চলে।

---

## 9. পরবর্তী ধাপ

পরবর্তী গুরুত্বপূর্ণ বিষয়:

- **Kafka Partitions**
- **Offsets**
- **Consumer Groups**

এগুলো বুঝলে **Production-level Kafka Architecture** সম্পর্কে ভালো ধারণা তৈরি হবে। 🚀
