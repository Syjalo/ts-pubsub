import {
  createAsyncGenerator,
  createReadableStream,
  createWritableStream,
  type SubscribeSource,
} from "./streams.js";
import type {
  StreamOptions,
  SubscribeOptions,
  TopicMap,
  TopicMessage,
  Unsubscribe,
  WildcardStreamOptions,
} from "./types.js";
import {
  isWildcardArgs,
  noopUnsubscribe,
  onAbort,
  toUnsubscribe,
} from "./utils.js";

interface TopicSubscription {
  subscriber: unknown;
  removeAbortListener: () => void;
  onClear?: () => void;
  dispatch: (args: unknown[]) => void;
}

interface WildcardSubscription<T extends Record<keyof T, unknown[]>> {
  onClear?: () => void;
  dispatch: (message: TopicMessage<T>) => void;
}

export class PubSub<
  T extends Record<keyof T, unknown[]> = TopicMap,
> implements Disposable {
  #topicSubscriptions = new Map<keyof T, Set<TopicSubscription>>();
  #wildcardSubscriptions = new Set<WildcardSubscription<T>>();

  public publish<K extends keyof T>(topic: K, ...args: T[K]): void {
    const subscriptions = this.#topicSubscriptions.get(topic);
    if (subscriptions) {
      for (const subscription of [...subscriptions]) {
        subscription.dispatch(args);
      }
    }

    if (this.#wildcardSubscriptions.size > 0) {
      const message = [topic, ...args] as TopicMessage<T>;
      for (const subscription of [...this.#wildcardSubscriptions]) {
        subscription.dispatch(message);
      }
    }
  }

  public subscribe<K extends keyof T>(
    topic: K,
    options?: StreamOptions<T[K]>,
  ): AsyncGenerator<T[K]>;
  public subscribe<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubscribeOptions<T[K]>,
  ): Unsubscribe;
  public subscribe<K extends keyof T>(
    topic: K,
    subscriberOrOptions?: ((...args: T[K]) => void) | StreamOptions<T[K]>,
    options?: SubscribeOptions<T[K]>,
  ): Unsubscribe | AsyncGenerator<T[K]> {
    if (typeof subscriberOrOptions !== "function") {
      return createAsyncGenerator(
        this.#createTopicSource(topic, subscriberOrOptions?.filter),
        subscriberOrOptions,
      );
    }

    return this.#addSubscription(topic, subscriberOrOptions, options);
  }

  public unsubscribe<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
  ): void {
    const subscriptions = this.#topicSubscriptions.get(topic);
    if (!subscriptions) return;

    for (const subscription of subscriptions) {
      if (subscription.subscriber === subscriber) {
        this.#removeSubscription(topic, subscriptions, subscription);
      }
    }
  }

  public once<K extends keyof T>(
    topic: K,
    options?: SubscribeOptions<T[K]>,
  ): Promise<T[K]>;
  public once<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubscribeOptions<T[K]>,
  ): Unsubscribe;
  public once<K extends keyof T>(
    topic: K,
    subscriberOrOptions?: ((...args: T[K]) => void) | SubscribeOptions<T[K]>,
    options?: SubscribeOptions<T[K]>,
  ): Promise<T[K]> | Unsubscribe {
    if (typeof subscriberOrOptions !== "function") {
      return this.#oncePromise(topic, subscriberOrOptions);
    }

    return this.#addSubscription(topic, subscriberOrOptions, {
      ...options,
      once: true,
    });
  }

  public clear(topic?: keyof T): void {
    const cleared: Array<{ onClear?: () => void }> = [];

    const detachTopic = (subscriptions: Set<TopicSubscription>) => {
      for (const subscription of subscriptions) {
        subscription.removeAbortListener();
        cleared.push(subscription);
      }
      subscriptions.clear();
    };

    if (topic !== undefined) {
      const subscriptions = this.#topicSubscriptions.get(topic);
      if (!subscriptions) return;
      this.#topicSubscriptions.delete(topic);
      detachTopic(subscriptions);
    } else {
      for (const subscriptions of this.#topicSubscriptions.values()) {
        detachTopic(subscriptions);
      }
      this.#topicSubscriptions.clear();
      cleared.push(...this.#wildcardSubscriptions);
      this.#wildcardSubscriptions.clear();
    }

    for (const subscription of cleared) {
      subscription.onClear?.();
    }
  }

  public subscriberCount(topic?: keyof T): number {
    if (topic !== undefined) {
      return this.#topicSubscriptions.get(topic)?.size ?? 0;
    }

    let total = this.#wildcardSubscriptions.size;
    for (const subscriptions of this.#topicSubscriptions.values()) {
      total += subscriptions.size;
    }
    return total;
  }

  public topics(): (keyof T)[] {
    return [...this.#topicSubscriptions.keys()];
  }

  public [Symbol.asyncIterator](): AsyncGenerator<TopicMessage<T>> {
    return createAsyncGenerator(this.#createWildcardSource());
  }

  public [Symbol.dispose](): void {
    this.clear();
  }

  public toReadable(
    options?: WildcardStreamOptions<T>,
  ): ReadableStream<TopicMessage<T>>;
  public toReadable<K extends keyof T>(
    topic: K,
    options?: StreamOptions<T[K]>,
  ): ReadableStream<T[K]>;
  public toReadable<K extends keyof T>(
    topicOrOptions?: K | WildcardStreamOptions<T>,
    options?: StreamOptions<T[K]>,
  ): ReadableStream<TopicMessage<T>> | ReadableStream<T[K]> {
    if (isWildcardArgs(topicOrOptions)) {
      return createReadableStream(
        this.#createWildcardSource(topicOrOptions?.filter),
        topicOrOptions,
      );
    }

    return createReadableStream(
      this.#createTopicSource(topicOrOptions, options?.filter),
      options,
    );
  }

  public toWritable(
    options?: WildcardStreamOptions<T>,
  ): WritableStream<TopicMessage<T>>;
  public toWritable<K extends keyof T>(
    topic: K,
    options?: StreamOptions<T[K]>,
  ): WritableStream<T[K]>;
  public toWritable<K extends keyof T>(
    topicOrOptions?: K | WildcardStreamOptions<T>,
    options?: StreamOptions<T[K]>,
  ): WritableStream<TopicMessage<T>> | WritableStream<T[K]> {
    if (isWildcardArgs(topicOrOptions)) {
      return createWritableStream<TopicMessage<T>>(
        ([messageTopic, ...args]) => {
          this.publish(messageTopic, ...args);
        },
        topicOrOptions,
      );
    }

    const filter = options?.filter;
    return createWritableStream<T[K]>(
      (args) => {
        this.publish(topicOrOptions, ...args);
      },
      options && {
        ...options,
        filter: filter ? (args) => filter(...args) : undefined,
      },
    );
  }

  #oncePromise<K extends keyof T>(
    topic: K,
    options?: SubscribeOptions<T[K]>,
  ): Promise<T[K]> {
    const { signal, filter } = options ?? {};
    if (signal?.aborted) {
      return Promise.reject(signal.reason);
    }

    return new Promise<T[K]>((resolve, reject) => {
      let settled = false;
      const trySettle = () => {
        if (settled) return false;
        settled = true;
        removeAbortListener();
        return true;
      };

      const unsubscribe = this.#addSubscription(
        topic,
        (...args) => {
          if (trySettle()) resolve(args);
        },
        {
          filter,
          once: true,
          onClear: () => {
            if (trySettle()) {
              reject(new DOMException("Subscription cleared", "AbortError"));
            }
          },
        },
      );

      const removeAbortListener = onAbort(signal, () => {
        if (trySettle()) {
          unsubscribe();
          reject(signal!.reason);
        }
      });
    });
  }

  #createTopicSource<K extends keyof T>(
    topic: K,
    filter?: (...args: T[K]) => boolean,
  ): SubscribeSource<T[K]> {
    return (push, onClear) =>
      this.#addSubscription(topic, (...args) => push(args), {
        filter,
        onClear,
      });
  }

  #createWildcardSource(
    filter?: (message: TopicMessage<T>) => boolean,
  ): SubscribeSource<TopicMessage<T>> {
    return (push, onClear) => {
      const subscription: WildcardSubscription<T> = {
        dispatch: (message) => {
          if (!this.#wildcardSubscriptions.has(subscription)) return;
          if (
            filter &&
            (!filter(message) || !this.#wildcardSubscriptions.has(subscription))
          ) {
            return;
          }
          push(message);
        },
        onClear,
      };

      this.#wildcardSubscriptions.add(subscription);
      return () => {
        this.#wildcardSubscriptions.delete(subscription);
      };
    };
  }

  #addSubscription<K extends keyof T>(
    topic: K,
    subscriber: (...args: T[K]) => void,
    options?: SubscribeOptions<T[K]> & { once?: boolean; onClear?: () => void },
  ): Unsubscribe {
    const { signal, filter, once = false, onClear } = options ?? {};
    if (signal?.aborted) return noopUnsubscribe;

    let subscriptions = this.#topicSubscriptions.get(topic);
    if (!subscriptions) {
      subscriptions = new Set();
      this.#topicSubscriptions.set(topic, subscriptions);
    }

    const remove = () =>
      this.#removeSubscription(topic, subscriptions, subscription);

    const subscription: TopicSubscription = {
      subscriber,
      removeAbortListener: onAbort(signal, remove),
      onClear,
      dispatch: (args: unknown[]) => {
        if (!subscriptions.has(subscription)) return;
        const typedArgs = args as T[K];
        if (
          filter &&
          (!filter(...typedArgs) || !subscriptions.has(subscription))
        ) {
          return;
        }
        if (once) remove();
        subscriber(...typedArgs);
      },
    };

    subscriptions.add(subscription);
    return toUnsubscribe(remove);
  }

  #removeSubscription(
    topic: keyof T,
    subscriptions: Set<TopicSubscription>,
    subscription: TopicSubscription,
  ): void {
    subscription.removeAbortListener();
    subscriptions.delete(subscription);
    if (
      subscriptions.size === 0 &&
      this.#topicSubscriptions.get(topic) === subscriptions
    ) {
      this.#topicSubscriptions.delete(topic);
    }
  }
}
