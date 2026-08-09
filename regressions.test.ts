/**
 * Regressions for defects found in the 3.0 audit. Each test names the behaviour that broke
 * and why it mattered, so a future change that reintroduces it fails here rather than in a
 * user's vault.
 *
 * These all exercise pure functions, which is why they need no Obsidian mock — the same
 * reason QueueManager.test.ts can test queue behaviour directly.
 */
import {
    sanitizeVaultPath,
    taskQueueId,
    compressText,
    decompressText,
    splitBinaryPayload,
    joinBinaryPayload,
    packFrame,
    unpackFrame,
} from './utils';
import { QueueManager } from './src/core/QueueManager';
import { TimeoutManager } from './src/utils/Timeouts';
import { SyncTask } from './types';

describe('sanitizeVaultPath', () => {
    it('accepts ordinary vault paths unchanged', () => {
        expect(sanitizeVaultPath('note.md')).toBe('note.md');
        expect(sanitizeVaultPath('Folder/Sub/note.md')).toBe('Folder/Sub/note.md');
        expect(sanitizeVaultPath('.obsidian/snippets/x.css')).toBe('.obsidian/snippets/x.css');
    });

    it('rejects traversal out of the vault', () => {
        expect(sanitizeVaultPath('../evil.md')).toBeNull();
        expect(sanitizeVaultPath('a/../../evil.md')).toBeNull();
        expect(sanitizeVaultPath('a/b/..')).toBeNull();
    });

    it('rejects absolute paths and drive letters', () => {
        expect(sanitizeVaultPath('/etc/passwd')).toBeNull();
        expect(sanitizeVaultPath('C:/Windows/system.ini')).toBeNull();
        expect(sanitizeVaultPath('//server/share/x')).toBeNull();
    });

    it('normalises "./" so it cannot slip past a prefix filter', () => {
        // The exclusion and .obsidian checks are startsWith() comparisons, so
        // './.obsidian/plugins/x' failed the '.obsidian/' test while still resolving into
        // the config folder — which is plugin code, i.e. arbitrary execution on restart.
        expect(sanitizeVaultPath('./.obsidian/plugins/x/main.js')).toBe('.obsidian/plugins/x/main.js');
        expect(sanitizeVaultPath('./Private/secrets.md')).toBe('Private/secrets.md');
        expect(sanitizeVaultPath('a//b/./c.md')).toBe('a/b/c.md');
    });

    it('rejects NUL bytes and non-strings', () => {
        expect(sanitizeVaultPath('a\0b.md')).toBeNull();
        expect(sanitizeVaultPath('')).toBeNull();
        expect(sanitizeVaultPath(undefined)).toBeNull();
        expect(sanitizeVaultPath(42)).toBeNull();
    });

    it('treats backslashes as separators rather than filename characters', () => {
        expect(sanitizeVaultPath('a\\b.md')).toBe('a/b.md');
        expect(sanitizeVaultPath('..\\evil.md')).toBeNull();
    });

    it('rejects trailing dots and spaces that Windows would strip', () => {
        // 'note.md ' and 'note.md' address the same file on Windows but compare unequal,
        // which would let a filtered path through under a slightly different spelling.
        expect(sanitizeVaultPath('note.md ')).toBeNull();
        expect(sanitizeVaultPath('folder./note.md')).toBeNull();
    });
});

describe('taskQueueId', () => {
    const batch = (paths: string[]): SyncTask => ({ taskType: 'send-file-batch', paths, batchId: 'b1' });

    it('gives each flush of one batch a distinct id', () => {
        // handleRequestBatch flushes a batch in several chunks that all carry the same
        // batchId. Keying on batchId alone made every flush after the first a duplicate,
        // and QueueManager drops duplicates silently — so those files were never sent and
        // the batch never completed, hanging the sync for the full 300 s timeout.
        const first = taskQueueId('peer', batch(['a.md', 'b.md']));
        const second = taskQueueId('peer', batch(['c.md', 'd.md']));
        expect(first).not.toBe(second);
    });

    it('still coalesces a genuinely identical re-queue', () => {
        expect(taskQueueId('peer', batch(['a.md']))).toBe(taskQueueId('peer', batch(['a.md'])));
    });

    it('separates tasks by peer, type and path', () => {
        const send: SyncTask = { taskType: 'send-file', path: 'a.md', mtime: 1, forceFull: false };
        expect(taskQueueId('p1', send)).not.toBe(taskQueueId('p2', send));
        expect(taskQueueId(null, send)).toBe(taskQueueId(null, send));
        expect(taskQueueId('p1', send)).not.toBe(
            taskQueueId('p1', { taskType: 'send-delete', path: 'a.md' })
        );
    });

    it('distinguishes renames that share one endpoint', () => {
        const a: SyncTask = { taskType: 'send-rename', oldPath: 'x.md', newPath: 'y.md' };
        const b: SyncTask = { taskType: 'send-rename', oldPath: 'x.md', newPath: 'z.md' };
        expect(taskQueueId('p', a)).not.toBe(taskQueueId('p', b));
    });
});

describe('QueueManager retry path', () => {
    it('retries an item whose processor reports failure', async () => {
        // The processor swallows its own errors, so the callback always returned true and
        // scheduleRetry was unreachable: item.retries never left 0 and a transient failure
        // silently lost the file.
        jest.useFakeTimers();
        const timeouts = new TimeoutManager();
        let attempts = 0;
        const qm = new QueueManager(timeouts, async () => {
            attempts++;
            return false;
        });

        qm.addToQueue({ id: 'x', peerId: null, retries: 0, priority: 1 });
        await Promise.resolve();
        expect(attempts).toBe(1);

        // Three retries at the fixed 5 s delay, then it stops.
        for (let i = 0; i < 5; i++) {
            await jest.advanceTimersByTimeAsync(5000);
        }
        expect(attempts).toBe(4);

        timeouts.clearAll();
        jest.useRealTimers();
    });

    it('does not retry an item that reports success', async () => {
        jest.useFakeTimers();
        const timeouts = new TimeoutManager();
        let attempts = 0;
        const qm = new QueueManager(timeouts, async () => {
            attempts++;
            return true;
        });

        qm.addToQueue({ id: 'y', peerId: null, retries: 0, priority: 1 });
        await Promise.resolve();
        await jest.advanceTimersByTimeAsync(20000);
        expect(attempts).toBe(1);

        timeouts.clearAll();
        jest.useRealTimers();
    });
});

describe('decompressText output cap', () => {
    it('round-trips normal content', () => {
        const text = 'hello '.repeat(1000);
        expect(decompressText(compressText(text))).toBe(text);
    });

    it('refuses a payload that expands past the limit', () => {
        // DEFLATE reaches roughly 1000:1, so an uncapped inflate let a few megabytes from a
        // peer expand to gigabytes. This frame is reachable before any authentication.
        const bomb = compressText('\0'.repeat(2 * 1024 * 1024));
        expect(() => decompressText(bomb, 1024)).toThrow(/Decompression failed/);
    });
});

describe('empty binary bodies', () => {
    it('round-trips a 0-byte file through the frame format', () => {
        // A zero-length body is indistinguishable from "no body" once framed, so the
        // receiver got content: undefined and threw on createBinary(path, undefined).
        const msg = {
            type: 'file-update',
            path: 'empty.bin',
            encoding: 'binary',
            mtime: 1,
            transferId: 't1',
            content: new ArrayBuffer(0),
        };

        const { header, body } = splitBinaryPayload(msg);
        const framed = packFrame(header, body);
        const unpacked = unpackFrame(framed.buffer.slice(framed.byteOffset, framed.byteOffset + framed.byteLength));
        const rebuilt = joinBinaryPayload(unpacked.header, unpacked.body);

        expect(rebuilt.content).toBeInstanceOf(ArrayBuffer);
        expect(rebuilt.content.byteLength).toBe(0);
        expect(rebuilt.__emptyBody).toBeUndefined();
    });

    it('still round-trips a non-empty binary body', () => {
        const bytes = new Uint8Array([1, 2, 3, 4]);
        const msg = {
            type: 'file-update',
            path: 'x.bin',
            encoding: 'binary',
            mtime: 1,
            transferId: 't2',
            content: bytes.buffer,
        };

        const { header, body } = splitBinaryPayload(msg);
        const framed = packFrame(header, body);
        const unpacked = unpackFrame(framed.buffer.slice(framed.byteOffset, framed.byteOffset + framed.byteLength));
        const rebuilt = joinBinaryPayload(unpacked.header, unpacked.body);

        expect(new Uint8Array(rebuilt.content)).toEqual(bytes);
    });
});
