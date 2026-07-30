import { TimeoutManager } from '../utils/Timeouts';
import { MAX_QUEUE_DEPTH } from '../../types';

export interface QueueItem {
    id?: string;
    peerId: string | null;
    task?: any;
    data?: any;
    retries: number;
    priority: number;
    /** Monotonic insertion counter, assigned internally to keep equal priorities FIFO. */
    seq?: number;
}

export class QueueManager {
    /**
     * Max-heap over priority, ties broken by insertion order.
     *
     * This was previously a sorted array with splice() insertion: O(n) element moves
     * per enqueue, so filling it with 10k items during a full sync cost on the order
     * of 50M moves. Heap insert/extract are O(log n).
     */
    private syncQueue: QueueItem[] = [];
    private activeQueueTransfers: number = 0;
    private pendingRetries: number = 0;
    private inQueueOrProcessing: Set<string> = new Set();
    private maxConcurrency: number = 3;
    private timeoutManager: TimeoutManager;
    private processCallback: (item: QueueItem) => Promise<boolean>;
    private syncDrainCallback: (() => void) | null = null;
    private queueIsPaused: boolean = false;
    // Incremented on clear(); pending retry timers from an older epoch must not re-add their items
    private epoch: number = 0;
    private seqCounter: number = 0;

    constructor(
        timeoutManager: TimeoutManager,
        processCallback: (item: QueueItem) => Promise<boolean>
    ) {
        this.timeoutManager = timeoutManager;
        this.processCallback = processCallback;
    }

    // --- Heap internals -----------------------------------------------------

    /** True when a sorts strictly before b (higher priority first, then earlier seq). */
    private precedes(a: QueueItem, b: QueueItem): boolean {
        if (a.priority !== b.priority) return a.priority > b.priority;
        return (a.seq ?? 0) < (b.seq ?? 0);
    }

    private heapPush(item: QueueItem) {
        const heap = this.syncQueue;
        heap.push(item);
        let i = heap.length - 1;
        while (i > 0) {
            const parent = (i - 1) >> 1;
            if (this.precedes(heap[i], heap[parent])) {
                [heap[i], heap[parent]] = [heap[parent], heap[i]];
                i = parent;
            } else break;
        }
    }

    private heapPop(): QueueItem | undefined {
        const heap = this.syncQueue;
        if (heap.length === 0) return undefined;
        const top = heap[0];
        const last = heap.pop()!;
        if (heap.length > 0) {
            heap[0] = last;
            let i = 0;
            for (;;) {
                const left = 2 * i + 1;
                const right = left + 1;
                let best = i;
                if (left < heap.length && this.precedes(heap[left], heap[best])) best = left;
                if (right < heap.length && this.precedes(heap[right], heap[best])) best = right;
                if (best === i) break;
                [heap[i], heap[best]] = [heap[best], heap[i]];
                i = best;
            }
        }
        return top;
    }

    // --- Public API --------------------------------------------------------

    public setConcurrencyLimit(limit: number) {
        this.maxConcurrency = limit;
    }

    public setSyncDrainCallback(callback: () => void) {
        this.syncDrainCallback = callback;
    }

    public pause() {
        this.queueIsPaused = true;
    }

    public resume() {
        this.queueIsPaused = false;
        this.processQueue();
    }

    public clear() {
        this.epoch++;
        this.syncQueue = [];
        this.inQueueOrProcessing.clear();
        // Do not reset activeQueueTransfers; let in-flight items finish naturally
    }

    public addToQueue(item: QueueItem) {
        // Callers are expected to supply a stable, content-derived id so that repeated
        // work for the same path coalesces. Only fall back to a random id when there is
        // nothing to key on — a random id can never dedup against anything.
        if (!item.id) {
            item.id = 'gen_' + Math.random().toString(36).substring(2, 11);
        }
        if (this.inQueueOrProcessing.has(item.id)) return;
        this.inQueueOrProcessing.add(item.id);

        if (item.seq === undefined) item.seq = this.seqCounter++;
        this.heapPush(item);
        this.processQueue();
    }

    public getQueuePressure(): number {
        return Math.min(1, (this.syncQueue.length + this.activeQueueTransfers) / MAX_QUEUE_DEPTH);
    }

    public getQueueSize(): number { return this.syncQueue.length; }
    public getActiveTransfers(): number { return this.activeQueueTransfers; }

    private processQueue() {
        if (this.queueIsPaused) return;
        // Drain the queue up to the concurrency limit. Since JS is single-threaded,
        // this loop runs atomically — no re-entry can occur before the while exits.
        while (this.activeQueueTransfers < this.maxConcurrency && this.syncQueue.length > 0) {
            const item = this.heapPop()!;
            this.activeQueueTransfers++;

            const scheduleRetry = () => {
                item.retries++;
                this.pendingRetries++;
                const scheduledEpoch = this.epoch;
                // Keep item.id in inQueueOrProcessing during the retry delay
                // to prevent duplicates from entering the queue in the window.
                this.timeoutManager.setTimeout(() => {
                    this.pendingRetries--;
                    if (item.id) this.inQueueOrProcessing.delete(item.id);
                    // If clear() ran while we were waiting, the item belongs to an
                    // aborted sync — don't resurrect it into the fresh queue.
                    if (scheduledEpoch === this.epoch) this.addToQueue(item);
                }, 5000);
            };

            this.processCallback(item)
                .then((success) => {
                    if (!success && item.retries < 3) {
                        scheduleRetry();
                    } else {
                        if (item.id) this.inQueueOrProcessing.delete(item.id);
                    }
                })
                .catch((e) => {
                    console.error("Queue item processing error", e);
                    if (item.retries < 3) {
                        scheduleRetry();
                    } else {
                        if (item.id) this.inQueueOrProcessing.delete(item.id);
                    }
                })
                .finally(() => {
                    this.activeQueueTransfers--;
                    // Re-enter processQueue after each item completes to drain pending work
                    this.processQueue();
                });
        }

        if (this.activeQueueTransfers === 0 && this.syncQueue.length === 0 && this.pendingRetries === 0 && this.syncDrainCallback) {
            this.syncDrainCallback();
        }
    }

    /** Snapshot of queued items. Heap order, not drain order — used only for persistence. */
    public getQueue(): QueueItem[] {
        return this.syncQueue;
    }

    public loadQueue(items: QueueItem[]) {
        if (!Array.isArray(items)) return;
        this.syncQueue = [];
        this.inQueueOrProcessing.clear();
        for (const item of items) {
            if (!item) continue;
            if (item.id) {
                if (this.inQueueOrProcessing.has(item.id)) continue;
                this.inQueueOrProcessing.add(item.id);
            }
            item.seq = this.seqCounter++;
            this.heapPush(item);
        }
        this.processQueue();
    }
}
