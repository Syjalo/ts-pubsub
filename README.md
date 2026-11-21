# @syjalo/pubsub

A lightweight, strictly-typed PubSub library for TypeScript and JavaScript with `AsyncGenerator`, `Promise`, `AbortSignal`, and Web Streams support. Zero runtime dependencies.

## Features

- **Strict TypeScript types** — Supports both `interface` and `type` event maps with tuple arguments.
- **Zero dependencies** — Built purely on standard Web & ECMAScript APIs (`AbortSignal`, `ReadableStream`, `WritableStream`, `AsyncGenerator`).
- **Flexible consumption** — Subscribe with callbacks, `await` single events with `once()`, iterate with `for await...of`, or pipe Web Streams.
- **Built-in cancellation** — First-class `AbortSignal` support for callbacks, promises, and async iterators.
- **Wildcard streaming** — Iterate or stream all topics at once as discriminated `[topic, ...args]` tuples.

## Installation

```bash
pnpm add @syjalo/pubsub
```

<details>
<summary>Other package managers</summary>

```bash
npm install @syjalo/pubsub
yarn add @syjalo/pubsub
bun add @syjalo/pubsub
```

</details>

## Quick Start

```ts
import { PubSub } from "@syjalo/pubsub";

interface Events {
  "user:login": [userId: string];
  "chat:message": [user: string, text: string];
  ping: [];
}

const pubsub = new PubSub<Events>();

// Subscribe with a callback
const unsubscribe = pubsub.sub("chat:message", (user, text) => {
  console.log(`${user}: ${text}`);
});

// Publish events
pubsub.pub("chat:message", "Alice", "Hello!");
pubsub.pub("ping");

// Unsubscribe
unsubscribe();
```

*(You can also use `new PubSub()` without a type parameter for untyped string topics.)*

## Usage

### `once()` — Callback or Promise

Wait for the next occurrence of a topic using either a callback or `await`:

```ts
// Callback style
pubsub.once("user:login", (userId) => {
  console.log("First login:", userId);
});

// Promise style
const [userId] = await pubsub.once("user:login");
console.log("Logged in:", userId);
```

### Cancellation with `AbortSignal`

Pass `{ signal }` to `sub()` or `once()` to automatically clean up subscriptions:

```ts
const controller = new AbortController();

// Callback subscription
pubsub.sub("ping", () => console.log("pong"), {
  signal: controller.signal,
});

// Promise-based once() rejects with signal.reason (or AbortError) when aborted
const loginPromise = pubsub.once("user:login", {
  signal: controller.signal,
});

controller.abort();
```

### Async Iteration (`for await...of`)

Calling `sub(topic)` without a callback returns an `AsyncGenerator` yielding event argument tuples:

```ts
const controller = new AbortController();

for await (const [user, text] of pubsub.sub("chat:message", {
  signal: controller.signal,
})) {
  console.log(`${user}: ${text}`);
}
```

Iterate over the `PubSub` instance directly to receive **all** topics as discriminated `[topic, ...args]` tuples:

```ts
for await (const message of pubsub) {
  if (message[0] === "chat:message") {
    const [, user, text] = message;
    console.log(`${user}: ${text}`);
  } else if (message[0] === "user:login") {
    const [, userId] = message;
    console.log("Login:", userId);
  }
}
```

### Web Streams (`ReadableStream` & `WritableStream`)

Convert any topic (or the entire bus) into standard Web Streams:

```ts
const busA = new PubSub<Events>();
const busB = new PubSub<Events>();

// Forward a single topic
busA.readable("chat:message").pipeTo(busB.writable("chat:message"));

// Or forward all topics
busA.readable().pipeTo(busB.writable());
```

Cancelling a `ReadableStream` reader automatically unsubscribes from the bus.

## API

### `new PubSub<T>()`

Creates a new `PubSub` instance. `T` maps topic names to argument tuple types (e.g. `Record<string, unknown[]>`).

### Methods

| Method | Description |
| --- | --- |
| `pub(topic, ...args)` | Publish an event to `topic` and any wildcard subscribers. |
| `sub(topic, fn, options?)` | Subscribe a callback to `topic`. Returns an `unsubscribe()` function. |
| `sub(topic, options?)` | Subscribe to `topic` as an `AsyncGenerator<T[K]>`. |
| `unsub(topic, fn)` | Remove `fn` (including `once` wrappers for `fn`) from `topic`. |
| `once(topic, fn, options?)` | Subscribe `fn` for a single event on `topic`. Returns an `unsubscribe()` function. |
| `once(topic, options?)` | Return a `Promise<T[K]>` that resolves on the next `topic` event (or rejects on abort). |
| `clear(topic?)` | Remove all subscribers for `topic`, or clear all topic and wildcard subscribers if omitted. |
| `count(topic?)` | Return the subscriber count for `topic`, or total subscribers (including wildcards) if omitted. |
| `topics()` | Return an array of topics that currently have subscribers. |
| `readable(topic?)` | Return a `ReadableStream` for `topic`, or all topics (`TopicMessage<T>`) if omitted. |
| `writable(topic?)` | Return a `WritableStream` that publishes to `topic`, or all topics if omitted. |
| `[Symbol.asyncIterator]()` | Return an `AsyncGenerator<TopicMessage<T>>` yielding all published events. |

## License

[MIT](./LICENSE) © [Synbulat Biishev](https://github.com/Syjalo)