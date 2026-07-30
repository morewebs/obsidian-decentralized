import { TimeoutManager } from '../utils/Timeouts';

/** Default WebRTC backpressure thresholds, in bytes. */
const DC_HIGH_WATER = 16 * 1024 * 1024;
const DC_LOW_WATER = 8 * 1024 * 1024;

export class ConnectionManager {
    private timeoutManager: TimeoutManager;

    constructor(timeoutManager: TimeoutManager) {
        this.timeoutManager = timeoutManager;
    }

    /**
     * Wait for a WebRTC data channel's send buffer to fall below lowWater.
     *
     * Uses addEventListener rather than assigning dc.onbufferedamountlow. The inline
     * copies of this logic that used to live in main.ts overwrote that handler and
     * restored it afterwards, so two concurrent waiters would clobber each other and
     * one could hang until its timeout.
     */
    public async waitForBufferToDrain(
        dc: any,
        maxWaitMs: number = 60000,
        highWater: number = DC_HIGH_WATER,
        lowWater: number = DC_LOW_WATER
    ): Promise<void> {
        if (!dc) return;
        if (dc.bufferedAmount <= highWater) return;

        return new Promise<void>((resolve, reject) => {
            let isResolved = false;

            const cleanup = () => {
                if (dc.removeEventListener) dc.removeEventListener('bufferedamountlow', handler);
            };

            const timeoutId = this.timeoutManager.setTimeout(() => {
                if (isResolved) return;
                isResolved = true;
                cleanup();
                reject(new Error("Timeout waiting for WebRTC buffer to drain"));
            }, maxWaitMs);

            const handler = () => {
                if (isResolved) return;
                // bufferedamountlow can fire while still above our own low water mark
                // if another writer raised the threshold; re-check before resolving.
                if (dc.bufferedAmount > lowWater) return;
                isResolved = true;
                this.timeoutManager.clearTimeout(timeoutId);
                cleanup();
                resolve();
            };

            dc.bufferedAmountLowThreshold = lowWater;

            if (dc.addEventListener) {
                dc.addEventListener('bufferedamountlow', handler);
            } else {
                // Fallback if addEventListener is unavailable (e.g. mock objects)
                dc.onbufferedamountlow = handler;
            }

            // The buffer may already have drained between the check above and the
            // listener being attached.
            if (dc.bufferedAmount <= lowWater) handler();
        });
    }

    /**
     * Direct-IP (WebSocket) equivalent. Node's ws exposes no drain event comparable to
     * bufferedamountlow, so this polls. Yields to the macrotask queue rather than
     * spinning via setImmediate, which starved rendering on large transfers.
     */
    public async waitForSocketToDrain(
        getBufferedAmount: () => number,
        highWater: number,
        lowWater: number,
        maxWaitMs: number = 60000
    ): Promise<void> {
        if (getBufferedAmount() <= highWater) return;

        const deadline = Date.now() + maxWaitMs;
        while (getBufferedAmount() > lowWater) {
            if (Date.now() > deadline) {
                throw new Error("Timeout waiting for socket buffer to drain");
            }
            await new Promise(resolve => setTimeout(resolve, 4));
        }
    }
}
