import { QueueManager, QueueItem } from './QueueManager';
import { TimeoutManager } from '../utils/Timeouts';

describe('QueueManager', () => {
    let timeoutManager: TimeoutManager;
    let processCallback: jest.Mock<Promise<boolean>, [QueueItem]>;
    let manager: QueueManager;

    beforeEach(() => {
        jest.useFakeTimers();
        timeoutManager = new TimeoutManager();
        processCallback = jest.fn().mockResolvedValue(true);
        manager = new QueueManager(timeoutManager, processCallback);
        manager.setConcurrencyLimit(2);
    });

    afterEach(() => {
        manager.clear();
        timeoutManager.clearAll();
        jest.useRealTimers();
    });

    test('should add items and process them by priority', async () => {
        // Pause to queue them up
        manager.pause();

        manager.addToQueue({ id: '1', peerId: 'A', retries: 0, priority: 1 });
        manager.addToQueue({ id: '2', peerId: 'B', retries: 0, priority: 10 });
        manager.addToQueue({ id: '3', peerId: 'C', retries: 0, priority: 5 });

        expect(manager.getQueueSize()).toBe(3);

        manager.resume();

        // Let the promises resolve
        for (let i = 0; i < 10; i++) await Promise.resolve();

        // Priority 10 should be processed first, then 5, then 1.
        expect(processCallback).toHaveBeenCalledTimes(3);
        expect(processCallback.mock.calls[0][0].id).toBe('2'); // Priority 10
        expect(processCallback.mock.calls[1][0].id).toBe('3'); // Priority 5
        expect(processCallback.mock.calls[2][0].id).toBe('1'); // Priority 1
    });

    test('should respect concurrency limit', async () => {
        let resolveItem1: (val: boolean) => void;
        let resolveItem2: (val: boolean) => void;
        let resolveItem3: (val: boolean) => void;

        processCallback.mockImplementationOnce(() => new Promise(r => resolveItem1 = r))
                       .mockImplementationOnce(() => new Promise(r => resolveItem2 = r))
                       .mockImplementationOnce(() => new Promise(r => resolveItem3 = r));

        manager.addToQueue({ id: '1', peerId: 'A', retries: 0, priority: 1 });
        manager.addToQueue({ id: '2', peerId: 'B', retries: 0, priority: 1 });
        manager.addToQueue({ id: '3', peerId: 'C', retries: 0, priority: 1 });

        // Only 2 should be active because concurrency limit is 2
        expect(manager.getActiveTransfers()).toBe(2);
        expect(manager.getQueueSize()).toBe(1);

        resolveItem1!(true);
        for (let i = 0; i < 10; i++) await Promise.resolve(); // allow microtask queue to process

        // Now item 3 should start
        expect(manager.getActiveTransfers()).toBe(2);
        expect(manager.getQueueSize()).toBe(0);

        resolveItem2!(true);
        resolveItem3!(true);
        for (let i = 0; i < 10; i++) await Promise.resolve();
    });

    test('should deduplicate items with the same id', () => {
        manager.pause();
        manager.addToQueue({ id: 'same', peerId: 'A', retries: 0, priority: 1 });
        manager.addToQueue({ id: 'same', peerId: 'B', retries: 0, priority: 2 });

        expect(manager.getQueueSize()).toBe(1);
        manager.resume();
    });

    test('should retry failed items', async () => {
        processCallback.mockResolvedValueOnce(false).mockResolvedValueOnce(true);

        manager.addToQueue({ id: 'retry-test', peerId: 'A', retries: 0, priority: 1 });

        for (let i = 0; i < 10; i++) await Promise.resolve(); // item fails

        expect(manager.getActiveTransfers()).toBe(0);
        expect(manager.getQueueSize()).toBe(0); // It's in pending retries

        jest.advanceTimersByTime(5000); // Trigger the retry timeout
        
        expect(manager.getQueueSize()).toBe(0); // Immediately picked up
        expect(manager.getActiveTransfers()).toBe(1);

        for (let i = 0; i < 10; i++) await Promise.resolve(); // item succeeds

        expect(manager.getActiveTransfers()).toBe(0);
    });

    test('heap drains strictly in priority order for a large shuffled batch', async () => {
        manager.pause();
        manager.setConcurrencyLimit(1);

        // Interleaved priorities, inserted in an order unrelated to drain order.
        const priorities: number[] = [];
        for (let i = 0; i < 500; i++) {
            const p = ((i * 37) % 101);
            priorities.push(p);
            manager.addToQueue({ id: `item-${i}`, peerId: 'A', retries: 0, priority: p });
        }
        expect(manager.getQueueSize()).toBe(500);

        manager.resume();
        for (let i = 0; i < 5000; i++) await Promise.resolve();

        expect(processCallback).toHaveBeenCalledTimes(500);
        const drained = processCallback.mock.calls.map(c => c[0].priority);
        const expected = [...priorities].sort((a, b) => b - a);
        expect(drained).toEqual(expected);
    });

    test('equal priorities drain FIFO', async () => {
        manager.pause();
        manager.setConcurrencyLimit(1);
        for (let i = 0; i < 20; i++) {
            manager.addToQueue({ id: `fifo-${i}`, peerId: 'A', retries: 0, priority: 7 });
        }
        manager.resume();
        for (let i = 0; i < 2000; i++) await Promise.resolve();

        const ids = processCallback.mock.calls.map(c => c[0].id);
        expect(ids).toEqual(Array.from({ length: 20 }, (_, i) => `fifo-${i}`));
    });

    test('repeated work for the same id collapses to a single queue entry', () => {
        manager.pause();
        for (let i = 0; i < 100; i++) {
            manager.addToQueue({ id: 'A send-file notes/note.md', peerId: 'A', retries: 0, priority: 50000 });
        }
        expect(manager.getQueueSize()).toBe(1);
        manager.resume();
    });

    test('loadQueue drops duplicate ids and preserves priority order', async () => {
        manager.pause();
        manager.setConcurrencyLimit(1);
        manager.loadQueue([
            { id: 'a', peerId: null, retries: 0, priority: 1, task: { taskType: 'send-delete', path: 'a' } },
            { id: 'b', peerId: null, retries: 0, priority: 100, task: { taskType: 'send-delete', path: 'b' } },
            { id: 'a', peerId: null, retries: 0, priority: 999, task: { taskType: 'send-delete', path: 'a' } },
        ]);
        expect(manager.getQueueSize()).toBe(2);

        manager.resume();
        for (let i = 0; i < 100; i++) await Promise.resolve();
        expect(processCallback.mock.calls.map(c => c[0].id)).toEqual(['b', 'a']);
    });

    test('should call syncDrainCallback when queue is empty and no retries pending', async () => {
        const drainCallback = jest.fn();
        manager.setSyncDrainCallback(drainCallback);

        processCallback.mockResolvedValueOnce(true);

        manager.addToQueue({ id: 'drain', peerId: 'A', retries: 0, priority: 1 });

        expect(drainCallback).not.toHaveBeenCalled();

        for (let i = 0; i < 10; i++) await Promise.resolve(); // item processes

        expect(drainCallback).toHaveBeenCalledTimes(1);
    });
});
