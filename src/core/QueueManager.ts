import { TimeoutManager } from '../utils/Timeouts';
import { MAX_QUEUE_DEPTH } from '../../types';

export interface QueueItem {
    id?: string;
    peerId: string | null;
    task?: any;
    data?: any;
    retries: number;
    priority: number;
}

export class QueueManager {
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

    constructor(
        timeoutManager: TimeoutManager, 
        processCallback: (item: QueueItem) => Promise<boolean>
    ) {
        this.timeoutManager = timeoutManager;
        this.processCallback = processCallback;
    }

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
        if (!item.id) {
            item.id = 'gen_' + Math.random().toString(36).substring(2, 11);
        }
        if (this.inQueueOrProcessing.has(item.id)) return;
        this.inQueueOrProcessing.add(item.id);
        
        let low = 0, high = this.syncQueue.length;
        while (low < high) {
            const mid = (low + high) >>> 1;
            if (this.syncQueue[mid].priority < item.priority) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        this.syncQueue.splice(low, 0, item);
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
            const item = this.syncQueue.shift()!;
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

    public getQueue(): QueueItem[] {
        return this.syncQueue;
    }

    public loadQueue(items: QueueItem[]) {
        if (!Array.isArray(items)) return;
        this.syncQueue = items;
        this.inQueueOrProcessing.clear();
        for (const item of items) {
            if (item.id) this.inQueueOrProcessing.add(item.id);
        }
        this.processQueue();
    }
}
