import type { Unsubscribe } from "./types.js";

export const noop = (): void => {};

export const noopUnsubscribe: Unsubscribe = toUnsubscribe(() => {});

export function onAbort(
  signal: AbortSignal | undefined,
  listener: () => void,
): () => void {
  if (!signal) return noop;
  signal.addEventListener("abort", listener, { once: true });
  return () => signal.removeEventListener("abort", listener);
}

export function isWildcardArgs<K, O>(
  topicOrOptions: K | O | undefined,
): topicOrOptions is O | undefined {
  return (
    topicOrOptions === undefined ||
    (typeof topicOrOptions === "object" && topicOrOptions !== null)
  );
}

export function toUnsubscribe(fn: () => void): Unsubscribe {
  (fn as Unsubscribe)[Symbol.dispose] = fn;
  return fn as Unsubscribe;
}
