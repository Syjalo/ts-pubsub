import type { DisposableAsyncGenerator, LimitOptions } from "./types.js";
import { abortError, noop, onAbort } from "./utils.js";

export type SubscribeSource<U> = (
  push: (item: U) => void,
  onClear: () => void,
) => () => void;

function normalizeMax(max: number | undefined): number | undefined {
  return max !== undefined && Number.isFinite(max) ? Math.floor(max) : max;
}

function hasExhaustedLimits(limits?: LimitOptions): boolean {
  if (!limits) return false;
  const { duration, idle } = limits;
  const max = normalizeMax(limits.max);
  return (
    (max !== undefined && !(max > 0)) ||
    (duration !== undefined && !(duration > 0)) ||
    (idle !== undefined && !(idle > 0))
  );
}

function createIterController(
  options: LimitOptions | undefined,
  onEnd: () => void,
  onAbortCallback: (reason: unknown) => void = onEnd,
) {
  const { signal, duration, idle } = options ?? {};
  const max = normalizeMax(options?.max);
  let count = 0;
  let disposed = false;
  let durationTimer: ReturnType<typeof setTimeout> | undefined;
  let idleTimer: ReturnType<typeof setTimeout> | undefined;
  let removeAbortListener = noop;

  const dispose = () => {
    if (disposed) return;
    disposed = true;
    clearTimeout(durationTimer);
    clearTimeout(idleTimer);
    removeAbortListener();
  };

  const triggerEnd = () => {
    dispose();
    onEnd();
  };

  const resetIdleTimer = () => {
    if (Number.isFinite(idle)) {
      clearTimeout(idleTimer);
      idleTimer = setTimeout(triggerEnd, idle);
    }
  };

  const start = () => {
    if (disposed) return;
    if (Number.isFinite(duration)) {
      durationTimer = setTimeout(triggerEnd, duration);
    }
    resetIdleTimer();
    removeAbortListener = onAbort(signal, () => {
      dispose();
      onAbortCallback(signal!.reason);
    });
  };

  const record = () => {
    if (disposed) return;
    if (max !== undefined && ++count >= max) {
      triggerEnd();
      return;
    }
    resetIdleTimer();
  };

  return { start, record, dispose };
}

export function createReadableStream<U>(
  subscribe: SubscribeSource<U>,
  options?: LimitOptions,
): ReadableStream<U> {
  let closed = false;
  let unsubscribe = noop;
  let iter: ReturnType<typeof createIterController> | undefined;

  const unsubscribeOnce = () => {
    const unsub = unsubscribe;
    unsubscribe = noop;
    unsub();
  };

  const cleanup = (): boolean => {
    if (closed) return false;
    closed = true;
    iter?.dispose();
    unsubscribeOnce();
    return true;
  };

  return new ReadableStream<U>({
    start: (controller) => {
      if (options?.signal?.aborted || hasExhaustedLimits(options)) {
        closed = true;
        controller.close();
        return;
      }

      const close = () => {
        if (cleanup()) controller.close();
      };

      const streamIter = createIterController(options, close);
      iter = streamIter;

      unsubscribe = subscribe((item) => {
        if (closed) return;
        controller.enqueue(item);
        if (!closed) streamIter.record();
      }, close);

      if (closed) {
        unsubscribeOnce();
      } else {
        streamIter.start();
      }
    },
    cancel: () => {
      cleanup();
    },
  });
}

export function createWritableStream<U>(
  publish: (chunk: U) => void,
  options?: LimitOptions & { filter?: (chunk: U) => boolean },
): WritableStream<U> {
  const { signal, filter } = options ?? {};
  let ended = hasExhaustedLimits(options);
  let iter: ReturnType<typeof createIterController> | undefined;

  const end = () => {
    if (ended) return;
    ended = true;
    iter?.dispose();
  };

  return new WritableStream<U>({
    start: (controller) => {
      if (signal?.aborted) {
        ended = true;
        controller.error(signal.reason);
        return;
      }

      if (ended) return;

      iter = createIterController(options, end, (reason) => {
        end();
        controller.error(reason);
      });
      iter.start();
    },
    write: (chunk) => {
      if (ended) {
        throw abortError("Cannot write: stream limits are exhausted");
      }
      try {
        if (filter && !filter(chunk)) return;
        publish(chunk);
      } catch (err) {
        end();
        throw err;
      }
      if (!ended) iter?.record();
    },
    close: end,
    abort: end,
  });
}

export function createAsyncGenerator<U>(
  subscribe: SubscribeSource<U>,
  options?: LimitOptions,
): DisposableAsyncGenerator<U> {
  if (options?.signal?.aborted || hasExhaustedLimits(options)) {
    const empty = (async function* () {})() as DisposableAsyncGenerator<U>;
    empty[Symbol.dispose] = noop;
    return empty;
  }

  let queue: U[] = [];
  let resolveNext: (() => void) | null = null;
  let ended = false;
  let aborted = false;
  let unsubscribe = noop;

  const wake = () => {
    resolveNext?.();
    resolveNext = null;
  };

  const unsubscribeOnce = () => {
    const unsub = unsubscribe;
    unsubscribe = noop;
    unsub();
  };

  const end = () => {
    if (ended) return;
    ended = true;
    iter.dispose();
    unsubscribeOnce();
    wake();
  };

  const abort = () => {
    if (aborted) return;
    aborted = true;
    queue = [];
    end();
  };

  const iter = createIterController(options, end, abort);

  unsubscribe = subscribe((item) => {
    if (ended) return;
    queue.push(item);
    iter.record();
    wake();
  }, end);

  if (ended) {
    unsubscribeOnce();
  } else {
    iter.start();
  }

  const generator = (async function* () {
    try {
      while (!aborted) {
        if (queue.length > 0) {
          const batch = queue;
          queue = [];
          for (const item of batch) {
            if (aborted) return;
            yield item;
          }
          continue;
        }

        if (ended) return;

        await new Promise<void>((resolve) => {
          resolveNext = resolve;
        });
      }
    } finally {
      abort();
    }
  })();

  const baseReturn = generator.return.bind(generator);
  generator.return = (value) => {
    abort();
    return baseReturn(value);
  };

  const baseThrow = generator.throw.bind(generator);
  generator.throw = (err) => {
    abort();
    return baseThrow(err);
  };

  const disposable = generator as DisposableAsyncGenerator<U>;
  disposable[Symbol.dispose] = () => {
    abort();
    baseReturn(undefined).catch(noop);
  };
  return disposable;
}
