# @syjalo/pubsub

A tiny, zero-dependency, strictly-typed PubSub library for TypeScript and JavaScript. Consume events via **callbacks**, **`Promise`**, **`for await...of`**, or **Web Streams** from a single bus.

## Features

- **Strictly typed event maps** — Works with both `interface` and `type` definitions (`Record<keyof T, unknown[]>`), or untyped with `new PubSub()`.
- **Four consumption models** — Callbacks (`subscribe`), one-shot promises (`once`), async iterators (`subscribe` / `for await (const msg of bus)`), and Web Streams (`toReadable` / `toWritable`).
- **Built-in flow & lifecycle controls** — Filter events (`filter`) and bound consumers with `signal`, `max`, `duration`, and `idle`.
- **Automatic cleanup (`using`)** — `PubSub` instances and `Unsubscribe` handles implement `Disposable`.
- **Encapsulated internals** — Uses ES `#private` fields so subclasses never collide with internal state.

## Installation

```sh
npm install @syjalo/pubsub
pnpm add @syjalo/pubsub
yarn add @syjalo/pubsub
bun add @syjalo/pubsub
```

## Quick Start

```ts
import { PubSub } from "@syjalo/pubsub";

interface Events {
  "user:signup": [userId: string, email: string];
  "order:paid": [orderId: string, amount: number];
}

const bus = new PubSub<Events>();

// 1. Callback subscription
const unsubscribe = bus.subscribe("order:paid", (orderId, amount) => {
  console.log(`Order ${orderId} paid: $${amount}`);
});

// 2. One-shot Promise with filter and timeout
const [orderId, amount] = await bus.once("order:paid", {
  filter: (id) => id === "ord_123",
  signal: AbortSignal.timeout(5_000),
});

// 3. Bounded async iteration
for await (const [userId, email] of bus.subscribe("user:signup", {
  max: 10,
  idle: 2_000,
})) {
  await sendWelcomeEmail(userId, email);
}

// 4. Web Streams
await bus
  .toReadable("order:paid", { max: 100 })
  .pipeTo(analyticsBus.toWritable("order:paid"));

// Publish an event
bus.publish("order:paid", "ord_123", 49.99);

unsubscribe();
```

For an untyped bus with `string` topics and `unknown[]` payloads, omit the generic parameter:

```ts
const bus = new PubSub();
```

## Guides

### Scoped Cleanup (`using` and `AbortSignal`)

Both `PubSub` and the `Unsubscribe` function returned by `subscribe()` / `once()` implement `Disposable`:

```ts
// Automatically unsubscribes when leaving the block
{
  using _ = bus.subscribe("order:paid", (id, amount) => {
    console.log(id, amount);
  });

  bus.publish("order:paid", "ord_1", 25);
}

// Automatically clears all listeners at the end of a test or scope
{
  using testBus = new PubSub<Events>();
  // ...
}
```

Use an `AbortSignal` to cancel multiple subscriptions, iterators, promises, or streams together:

```ts
const controller = new AbortController();

bus.subscribe("order:paid", handleOrder, { signal: controller.signal });
bus.subscribe("user:signup", handleSignup, { signal: controller.signal });

controller.abort();
```

### Wildcard Iteration & Streams

Iterate over the bus directly to observe every topic as a discriminated `[topic, ...args]` tuple (`TopicMessage<T>`):

```ts
for await (const [topic, ...args] of bus) {
  switch (topic) {
    case "order:paid": {
      const [orderId, amount] = args;
      console.log(orderId, amount);
      break;
    }
    case "user:signup": {
      const [userId, email] = args;
      console.log(userId, email);
      break;
    }
  }
}
```

`toReadable()` and `toWritable()` also accept no topic to stream `TopicMessage<T>` values across all topics:

```ts
// Stream matching events as Server-Sent Events (SSE)
export function GET(req: Request) {
  const body = bus
    .toReadable("order:paid", { signal: req.signal, idle: 60_000 })
    .pipeThrough(
      new TransformStream<Events["order:paid"], string>({
        transform([orderId, amount], controller) {
          controller.enqueue(`data: ${JSON.stringify({ orderId, amount })}\n\n`);
        },
      }),
    )
    .pipeThrough(new TextEncoderStream());

  return new Response(body, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
    },
  });
}
```

### Limits, Filtering, and Lifecycle Semantics

Async iterators and streams share `LimitOptions` (`signal`, `max`, `duration`, `idle`):

- **`filter`**: Only events where `filter(...)` returns `true` are delivered and counted toward `max` or used to reset `idle`.
- **`max`**: Ends consumption after receiving `max` matching events.
- **`duration`**: Ends consumption after `duration` milliseconds from start.
- **`idle`**: Resets on each matching event and ends consumption if no matching event arrives within `idle` milliseconds.
- Values `<= 0` or `NaN` for `max`, `duration`, or `idle` end immediately; `Infinity` disables that limit.

When consumption ends:

- **`clear()` / `clear(topic)`**:
  - Active `subscribe()` async iterators drain already-queued items and finish.
  - Active `toReadable()` streams close cleanly.
  - Pending `once()` promises reject with `DOMException("Subscription cleared", "AbortError")`.
- **`signal` abort**:
  - Pending `once()` promises reject with `signal.reason`.
  - Active `subscribe()` async iterators discard queued items and stop immediately.
  - Active `toReadable()` streams close cleanly.
  - Active `toWritable()` streams error with `signal.reason`.

## API Reference

### `PubSub<T = TopicMap>`

| Member | Description |
| --- | --- |
| `publish(topic, ...args)` | Publish `args` to subscribers of `topic` and wildcard consumers. |
| `subscribe(topic, fn, options?)` | Register a callback subscriber; returns a callable & `Disposable` `Unsubscribe`. |
| `subscribe(topic, options?)` | Return an `AsyncGenerator<T[K]>` yielding matching event argument tuples. |
| `unsubscribe(topic, fn)` | Remove all registrations of `fn` on `topic` (including `once` and filtered listeners). |
| `once(topic, fn, options?)` | Invoke `fn` at most once on the next matching event; returns `Unsubscribe`. |
| `once(topic, options?)` | Return a `Promise<T[K]>` resolving with the next matching event's argument tuple. |
| `clear(topic?)` | Remove all subscriptions for `topic`, or all topic and wildcard subscriptions if omitted. |
| `subscriberCount(topic?)` | Return the subscriber count for `topic`, or total topic + wildcard subscriptions if omitted. |
| `topics()` | Return an array of topics that currently have active topic subscriptions. |
| `toReadable(options?)` | Create a `ReadableStream<TopicMessage<T>>` over all topics. |
| `toReadable(topic, options?)` | Create a `ReadableStream<T[K]>` for a specific topic. |
| `toWritable(options?)` | Create a `WritableStream<TopicMessage<T>>` that publishes `[topic, ...args]` chunks. |
| `toWritable(topic, options?)` | Create a `WritableStream<T[K]>` that publishes argument tuples to `topic`. |
| `[Symbol.asyncIterator]()` | Return an `AsyncGenerator<TopicMessage<T>>` over all published events. |
| `[Symbol.dispose]()` | Alias for `clear()`. |

### Exported Types

| Type | Definition |
| --- | --- |
| `TopicMap` | `Record<string, unknown[]>` |
| `TopicMessage<T>` | `{ [K in keyof T]: [K, ...T[K]] }[keyof T]` |
| `Unsubscribe` | `(() => void) & Disposable` |
| `LimitOptions` | `{ signal?: AbortSignal; max?: number; duration?: number; idle?: number }` |
| `SubscribeOptions<Args>` | `{ signal?: AbortSignal; filter?: (...args: Args) => boolean }` |
| `StreamOptions<Args>` | `SubscribeOptions<Args> & LimitOptions` |
| `WildcardStreamOptions<T>` | `LimitOptions & { filter?: (message: TopicMessage<T>) => boolean }` |

## License

[MIT](./LICENSE) © [Synbulat Biishev](https://github.com/Syjalo)