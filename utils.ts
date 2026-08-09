// @ts-ignore
import * as pako from 'pako';
import { SyncError, SyncErrorCategory, SyncTask } from './types';

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function compressText(content: string): ArrayBuffer {
    try {
        const result = pako.deflate(content);
        // Fix: Slice the buffer to the exact compressed length instead of returning the entire underlying slab buffer.
        return result.buffer.slice(result.byteOffset, result.byteOffset + result.byteLength);
    } catch (err: any) {
        throw new SyncError(
            SyncErrorCategory.INTEGRITY_ERROR,
            `Compression failed: ${err.message || err}`,
            true,
            'Check if the file content is valid text and retry.'
        );
    }
}

/**
 * Ceiling on what a single compressed payload may expand to. DEFLATE reaches roughly
 * 1000:1, so without a cap a few megabytes from a peer inflate to gigabytes and take the
 * app down. 256 MB is far above any real note or attachment we compress.
 */
export const MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024;

export function decompressText(data: ArrayBuffer, maxBytes: number = MAX_DECOMPRESSED_BYTES): string {
    try {
        // pako has no output-size option, so inflate in chunks and stop as soon as the
        // running total crosses the limit rather than after the allocation has happened.
        const inflater = new pako.Inflate({ to: 'string' });
        let total = 0;
        let overflowed = false;
        (inflater as any).onData = function (chunk: string) {
            total += chunk.length;
            if (total > maxBytes) {
                overflowed = true;
                return;
            }
            (this as any).chunks.push(chunk);
        };
        inflater.push(new Uint8Array(data), true);

        if (overflowed) {
            throw new Error(`decompressed payload exceeds the ${Math.round(maxBytes / (1024 * 1024))} MB limit`);
        }
        if (inflater.err) {
            throw new Error(inflater.msg || `inflate error ${inflater.err}`);
        }
        return (inflater.result as string) ?? '';
    } catch (err: any) {
        throw new SyncError(
            SyncErrorCategory.INTEGRITY_ERROR,
            `Decompression failed: ${err.message || err}`,
            false,
            'The transferred file data might be corrupted. Please re-sync.'
        );
    }
}

/**
 * Normalises a vault-relative path that came from a peer and rejects anything that could
 * escape the vault or slip past the folder filters.
 *
 * The filters elsewhere are prefix comparisons, so `./.obsidian/plugins/x` did not match a
 * `.obsidian/` check while still resolving into the config directory — enough to overwrite
 * plugin code. Returns null when the path is not safe to use.
 */
export function sanitizeVaultPath(rawPath: unknown): string | null {
    if (typeof rawPath !== 'string') return null;

    // Backslashes are path separators on Windows; normalise so one syntax is validated.
    let path = rawPath.replace(/\\/g, '/');

    if (path.length === 0 || path.length > 1024) return null;
    if (path.includes('\0')) return null;
    // Absolute POSIX paths, Windows drive letters, and UNC paths.
    if (path.startsWith('/') || /^[a-zA-Z]:/.test(path) || path.startsWith('//')) return null;

    const segments: string[] = [];
    for (const segment of path.split('/')) {
        // Collapse empty segments (from `a//b`) and `.` rather than rejecting outright:
        // they are meaningless but not hostile.
        if (segment === '' || segment === '.') continue;
        // `..` is never resolved, only rejected — resolving it would let a peer probe how
        // deep the path is and still climb out of any folder it was restricted to.
        if (segment === '..') return null;
        // Trailing dots and spaces are stripped by Windows, so `foo.md ` and `foo.md`
        // address the same file while comparing as different strings.
        if (segment !== segment.replace(/[. ]+$/, '')) return null;
        segments.push(segment);
    }

    if (segments.length === 0) return null;
    return segments.join('/');
}

export function arrayBufferToBase64(buffer: ArrayBuffer): string {
    try {
        let binary = '';
        const bytes = new Uint8Array(buffer);
        const len = bytes.byteLength;
        const chunkSize = 8192;
        for (let i = 0; i < len; i += chunkSize) {
            binary += String.fromCharCode.apply(null, Array.from(bytes.subarray(i, i + chunkSize)));
        }
        return window.btoa(binary);
    } catch (err: any) {
        throw new SyncError(
            SyncErrorCategory.INTEGRITY_ERROR,
            `Base64 encoding failed: ${err.message || err}`,
            true,
            'Verify the input buffer size/content and retry.'
        );
    }
}

export function base64ToArrayBuffer(base64: string): ArrayBuffer {
    try {
        const binary_string = window.atob(base64);
        const len = binary_string.length;
        const bytes = new Uint8Array(len);
        for (let i = 0; i < len; i++) {
            bytes[i] = binary_string.charCodeAt(i);
        }
        return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    } catch (err: any) {
        throw new SyncError(
            SyncErrorCategory.INTEGRITY_ERROR,
            `Base64 decoding failed: ${err.message || err}`,
            false,
            'Invalid base64 string provided. Check file transfer integrity.'
        );
    }
}

/**
 * Run `worker` over every item with at most `limit` in flight.
 *
 * Used where the previous code either awaited serially inside a loop (slow) or mapped
 * everything into Promise.allSettled at once (unbounded — up to 500 concurrent vault
 * writes during a batch apply). Never rejects: each result carries its own outcome.
 */
export async function mapWithConcurrency<T, R>(
    items: T[],
    limit: number,
    worker: (item: T, index: number) => Promise<R>
): Promise<PromiseSettledResult<R>[]> {
    const results: PromiseSettledResult<R>[] = new Array(items.length);
    let next = 0;

    const runners = new Array(Math.min(Math.max(1, limit), items.length)).fill(0).map(async () => {
        for (;;) {
            const i = next++;
            if (i >= items.length) return;
            try {
                results[i] = { status: 'fulfilled', value: await worker(items[i], i) };
            } catch (reason) {
                results[i] = { status: 'rejected', reason };
            }
        }
    });

    await Promise.all(runners);
    return results;
}

/**
 * Wire protocol version. Bumped for V3, which replaced the base64-in-JSON
 * encryption envelope with a binary frame. A V3 peer cannot talk to a 2.x peer,
 * so the handshake refuses mismatched versions rather than corrupting a vault.
 */
export const PROTOCOL_VERSION = 3;

/** Messages that carry a large binary body in a single named field. */
const BINARY_BODY_FIELD: Record<string, 'data' | 'content'> = {
    'file-chunk-data': 'data',
    'file-batch-binary': 'data',
    'encrypted-frame': 'data',
    'sync-control-binary': 'data',
    'file-update': 'content',
};

/**
 * Split a message into its small JSON header and its bulk binary body, so the
 * body can be carried as raw bytes instead of being base64'd into the JSON.
 * Returns a null body for messages that have no binary payload.
 */
export function splitBinaryPayload(msg: any): { header: any; body: Uint8Array | null } {
    const field = msg && typeof msg === 'object' ? BINARY_BODY_FIELD[msg.type] : undefined;
    if (!field) return { header: msg, body: null };

    const value = msg[field];
    if (!(value instanceof ArrayBuffer) && !(value instanceof Uint8Array)) {
        return { header: msg, body: null };
    }
    // 'file-update' only travels as binary when its encoding says so; a utf8
    // update keeps its string content in the header.
    if (msg.type === 'file-update' && msg.encoding !== 'binary' && msg.encoding !== 'base64') {
        return { header: msg, body: null };
    }

    const { [field]: _omitted, ...header } = msg;
    const body = value instanceof Uint8Array ? value : new Uint8Array(value);
    // An empty body is indistinguishable from "no body at all" once framed, because the
    // frame carries only a header length. Without this marker a 0-byte file arrived with an
    // undefined content field and the receiver threw on createBinary(path, undefined).
    if (body.byteLength === 0) header.__emptyBody = true;
    return { header, body };
}

/** Reattach a binary body to the field its message type expects. */
export function joinBinaryPayload(header: any, body: ArrayBuffer | null): any {
    if (!header || typeof header !== 'object') return header;
    const field = BINARY_BODY_FIELD[header.type];
    if (!field) return header;

    if (header.__emptyBody) {
        delete header.__emptyBody;
        header[field] = new ArrayBuffer(0);
        return header;
    }
    if (body) header[field] = body;
    return header;
}

/**
 * Pack a header + optional binary body into one buffer:
 *   [4B headerLen LE][header JSON UTF-8][body bytes]
 * This is the plaintext form; the encrypted path encrypts the whole thing at once.
 */
export function packFrame(header: any, body: Uint8Array | null): Uint8Array {
    const headerBytes = textEncoder.encode(JSON.stringify(header));
    const bodyLen = body ? body.byteLength : 0;
    const out = new Uint8Array(4 + headerBytes.byteLength + bodyLen);
    new DataView(out.buffer).setUint32(0, headerBytes.byteLength, true);
    out.set(headerBytes, 4);
    if (body) out.set(body, 4 + headerBytes.byteLength);
    return out;
}

/** Inverse of packFrame. Throws a PROTOCOL_ERROR SyncError on a truncated frame. */
export function unpackFrame(buffer: ArrayBuffer): { header: any; body: ArrayBuffer | null } {
    if (buffer.byteLength < 4) {
        throw new SyncError(
            SyncErrorCategory.PROTOCOL_ERROR,
            `Frame too short (${buffer.byteLength} bytes)`,
            false,
            'Re-request the message.'
        );
    }
    const headerLen = new DataView(buffer).getUint32(0, true);
    if (4 + headerLen > buffer.byteLength) {
        throw new SyncError(
            SyncErrorCategory.PROTOCOL_ERROR,
            `Frame header length (${headerLen}) exceeds buffer size (${buffer.byteLength})`,
            false,
            'Re-request the message.'
        );
    }
    const header = JSON.parse(textDecoder.decode(new Uint8Array(buffer, 4, headerLen)));
    const bodyStart = 4 + headerLen;
    const body = bodyStart < buffer.byteLength ? buffer.slice(bodyStart) : null;
    return { header, body };
}

/**
 * Stable identity for a queued task, so re-queueing the same work for the same peer
 * coalesces instead of piling up. QueueManager dedups on this id and drops duplicates
 * silently, so anything that must not collapse has to be distinguishable here.
 *
 * NUL is the field separator: it is the one byte that cannot appear in a vault path.
 */
export function taskQueueId(peerId: string | null, task: SyncTask): string {
    // A batch is flushed in several chunks that all share one batchId, so the paths have to
    // be part of the id. Keying on batchId alone made every flush after the first a
    // duplicate, and those files were silently never sent.
    const target = task.taskType === 'send-rename'
        ? `${task.oldPath}\0${task.newPath}`
        : (task.taskType === 'send-file-batch' ? `${task.batchId}\0${task.paths.join('\0')}` : task.path);
    return `${peerId || '*'}\0${task.taskType}\0${target}`;
}

export interface PackedFile {
    path: string;
    mtime: number;
    isCompressed: boolean;
    encoding: 'utf8' | 'binary' | 'base64';
    content: ArrayBuffer | Uint8Array;
}

export function packFilesToTLV(files: PackedFile[]): ArrayBuffer {
    try {
        let totalSize = 0;
        
        // Calculate total size first to avoid reallocations
        const encodedPaths: Uint8Array[] = [];
        for (const file of files) {
            const pathEncoded = textEncoder.encode(file.path);
            encodedPaths.push(pathEncoded);
            // 2 (path len) + pathBytes + 8 (mtime) + 1 (isCompressed) + 1 (encoding) + 4 (content len) + contentBytes
            totalSize += 2 + pathEncoded.byteLength + 8 + 1 + 1 + 4 + file.content.byteLength;
        }
        
        const buffer = new ArrayBuffer(totalSize);
        const view = new DataView(buffer);
        const bytes = new Uint8Array(buffer);
        let offset = 0;
        
        for (let i = 0; i < files.length; i++) {
            const file = files[i];
            const pathEncoded = encodedPaths[i];
            
            view.setUint16(offset, pathEncoded.byteLength, true); offset += 2;
            bytes.set(pathEncoded, offset); offset += pathEncoded.byteLength;
            
            view.setFloat64(offset, file.mtime, true); offset += 8;
            
            view.setUint8(offset, file.isCompressed ? 1 : 0); offset += 1;
            
            let encFlag = 0;
            if (file.encoding === 'binary') encFlag = 1;
            else if (file.encoding === 'base64') encFlag = 2;
            view.setUint8(offset, encFlag); offset += 1;
            
            view.setUint32(offset, file.content.byteLength, true); offset += 4;
            const contentBytes = file.content instanceof Uint8Array ? file.content : new Uint8Array(file.content);
            bytes.set(contentBytes, offset); offset += file.content.byteLength;
        }
        
        return buffer;
    } catch (err: any) {
        throw new SyncError(
            SyncErrorCategory.PROTOCOL_ERROR,
            `TLV packing failed: ${err.message || err}`,
            true,
            'Verify files schema and retry sync.'
        );
    }
}

export function unpackTLVToFiles(buffer: ArrayBuffer): PackedFile[] {
    try {
        const view = new DataView(buffer);
        const files: PackedFile[] = [];
        let offset = 0;
        
        while (offset < buffer.byteLength) {
            // 1. Check if we can read the path length (2 bytes)
            if (offset + 2 > buffer.byteLength) {
                throw new SyncError(
                    SyncErrorCategory.PROTOCOL_ERROR,
                    `TLV unpack failed: Truncated buffer when reading path length at offset ${offset}`,
                    false,
                    'Re-request the file sync batch.'
                );
            }
            const pathLen = view.getUint16(offset, true); offset += 2;
            
            // 2. Check if we can read the path bytes (pathLen bytes)
            if (offset + pathLen > buffer.byteLength) {
                throw new SyncError(
                    SyncErrorCategory.PROTOCOL_ERROR,
                    `TLV unpack failed: Truncated buffer when reading path of length ${pathLen} at offset ${offset}`,
                    false,
                    'Re-request the file sync batch.'
                );
            }
            const pathBytes = new Uint8Array(buffer, offset, pathLen);
            const path = textDecoder.decode(pathBytes); offset += pathLen;
            
            // 3. Check if we can read mtime (8 bytes), isCompressed (1 byte), encFlag (1 byte), and contentLen (4 bytes)
            if (offset + 14 > buffer.byteLength) {
                throw new SyncError(
                    SyncErrorCategory.PROTOCOL_ERROR,
                    `TLV unpack failed: Truncated buffer when reading metadata at offset ${offset}`,
                    false,
                    'Re-request the file sync batch.'
                );
            }
            const mtime = view.getFloat64(offset, true); offset += 8;
            const isCompressed = view.getUint8(offset) === 1; offset += 1;
            const encFlag = view.getUint8(offset); offset += 1;
            
            let encoding: 'utf8' | 'binary' | 'base64' = 'utf8';
            if (encFlag === 1) encoding = 'binary';
            else if (encFlag === 2) encoding = 'base64';
            
            const contentLen = view.getUint32(offset, true); offset += 4;
            
            // 4. Check if we can read content bytes (contentLen bytes)
            if (offset + contentLen > buffer.byteLength) {
                throw new SyncError(
                    SyncErrorCategory.PROTOCOL_ERROR,
                    `TLV unpack failed: Truncated buffer when reading content of length ${contentLen} at offset ${offset}`,
                    false,
                    'Re-request the file sync batch.'
                );
            }
            // Use slice to create an independent copy and prevent retaining the entire batch buffer
            const content = new Uint8Array(buffer.slice(offset, offset + contentLen));
            offset += contentLen;
            
            files.push({ path, mtime, isCompressed, encoding, content });
        }
        
        return files;
    } catch (err: any) {
        if (err instanceof SyncError) {
            throw err;
        }
        throw new SyncError(
            SyncErrorCategory.PROTOCOL_ERROR,
            `TLV unpacking failed: ${err.message || err}`,
            false,
            'Re-request the file sync batch.'
        );
    }
}
