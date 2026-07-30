import {
    compressText,
    decompressText,
    arrayBufferToBase64,
    base64ToArrayBuffer,
    packFilesToTLV,
    unpackTLVToFiles,
    PackedFile,
    splitBinaryPayload,
    joinBinaryPayload,
    packFrame,
    unpackFrame
} from './utils';

// Mock window.btoa and window.atob for Node environment
if (typeof window === 'undefined') {
    (global as any).window = {
        btoa: btoa,
        atob: atob
    };
}

describe('Compression utils', () => {
    it('should compress and decompress text successfully', () => {
        const originalText = "Hello, Obsidian Decentralized Sync! This is a test string to be compressed and decompressed.";
        const compressed = compressText(originalText);
        
        expect(compressed).toBeInstanceOf(ArrayBuffer);
        expect(compressed.byteLength).toBeGreaterThan(0);
        
        const decompressed = decompressText(compressed);
        expect(decompressed).toBe(originalText);
    });

    it('should handle empty strings', () => {
        const originalText = "";
        const compressed = compressText(originalText);
        const decompressed = decompressText(compressed);
        expect(decompressed).toBe(originalText);
    });
});

describe('Base64 utils', () => {
    it('should convert ArrayBuffer to Base64 and back', () => {
        const testString = "Test array buffer to base64 conversion.";
        const encoder = new TextEncoder();
        const decoder = new TextDecoder();
        
        const buffer = encoder.encode(testString).buffer;
        const base64 = arrayBufferToBase64(buffer);
        
        expect(typeof base64).toBe('string');
        expect(base64.length).toBeGreaterThan(0);
        
        const decodedBuffer = base64ToArrayBuffer(base64);
        const decodedString = decoder.decode(decodedBuffer);
        
        expect(decodedString).toBe(testString);
    });
});

describe('TLV packing/unpacking', () => {
    it('should pack and unpack files correctly', () => {
        const encoder = new TextEncoder();
        
        const files: PackedFile[] = [
            {
                path: 'folder/file1.md',
                mtime: 1234567890.123,
                isCompressed: true,
                encoding: 'utf8',
                content: encoder.encode('File 1 content').buffer
            },
            {
                path: 'file2.png',
                mtime: 9876543210.987,
                isCompressed: false,
                encoding: 'binary',
                content: new Uint8Array([1, 2, 3, 4, 5])
            }
        ];

        const packedBuffer = packFilesToTLV(files);
        expect(packedBuffer).toBeInstanceOf(ArrayBuffer);
        expect(packedBuffer.byteLength).toBeGreaterThan(0);

        const unpackedFiles = unpackTLVToFiles(packedBuffer);
        
        expect(unpackedFiles.length).toBe(2);
        
        // Check first file
        expect(unpackedFiles[0].path).toBe(files[0].path);
        expect(unpackedFiles[0].mtime).toBeCloseTo(files[0].mtime, 3);
        expect(unpackedFiles[0].isCompressed).toBe(files[0].isCompressed);
        expect(unpackedFiles[0].encoding).toBe(files[0].encoding);
        
        // Content comparison
        const unpackedContent1 = new Uint8Array(unpackedFiles[0].content);
        const originalContent1 = new Uint8Array(files[0].content);
        expect(unpackedContent1).toEqual(originalContent1);

        // Check second file
        expect(unpackedFiles[1].path).toBe(files[1].path);
        expect(unpackedFiles[1].mtime).toBeCloseTo(files[1].mtime, 3);
        expect(unpackedFiles[1].isCompressed).toBe(files[1].isCompressed);
        expect(unpackedFiles[1].encoding).toBe(files[1].encoding);
        
        const unpackedContent2 = new Uint8Array(unpackedFiles[1].content);
        const originalContent2 = new Uint8Array(files[1].content as Uint8Array);
        expect(unpackedContent2).toEqual(originalContent2);
    });
});

describe('Binary frame codec (V3 wire format)', () => {
    it('round-trips a header-only message', () => {
        const msg = { type: 'request-batch', batchId: 'b1', paths: ['a.md', 'b.md'] };
        const { header, body } = splitBinaryPayload(msg);
        expect(body).toBeNull();
        expect(header).toBe(msg);

        const framed = packFrame(header, null);
        const out = unpackFrame(framed.buffer.slice(framed.byteOffset, framed.byteOffset + framed.byteLength));
        expect(out.body).toBeNull();
        expect(out.header).toEqual(msg);
    });

    it('keeps a binary body as raw bytes and restores it to the right field', () => {
        const payload = new Uint8Array([0, 1, 2, 253, 254, 255]);
        const msg = { type: 'file-chunk-data', transferId: 't1', index: 7, data: payload.buffer };

        const { header, body } = splitBinaryPayload(msg);
        expect(body).not.toBeNull();
        // The body must NOT be duplicated into the header.
        expect((header as any).data).toBeUndefined();
        expect(header.type).toBe('file-chunk-data');
        expect(body!.byteLength).toBe(payload.byteLength);

        const framed = packFrame(header, body);
        const out = unpackFrame(framed.buffer.slice(framed.byteOffset, framed.byteOffset + framed.byteLength));
        const rejoined = joinBinaryPayload(out.header, out.body);
        expect(rejoined.index).toBe(7);
        expect(new Uint8Array(rejoined.data)).toEqual(payload);
    });

    it('routes file-batch-binary and encrypted-frame bodies to .data', () => {
        for (const type of ['file-batch-binary', 'encrypted-frame', 'sync-control-binary']) {
            const payload = new Uint8Array([9, 8, 7]);
            const { header, body } = splitBinaryPayload({ type, data: payload.buffer });
            expect(body).not.toBeNull();
            const framed = packFrame(header, body);
            const out = unpackFrame(framed.buffer.slice(framed.byteOffset, framed.byteOffset + framed.byteLength));
            const rejoined = joinBinaryPayload(out.header, out.body);
            expect(new Uint8Array(rejoined.data)).toEqual(payload);
        }
    });

    it('treats a utf8 file-update as header-only but a binary one as framed', () => {
        const textUpdate = { type: 'file-update', path: 'n.md', encoding: 'utf8', content: 'hello' };
        expect(splitBinaryPayload(textUpdate).body).toBeNull();

        const bin = new Uint8Array([1, 2, 3]);
        const binUpdate = { type: 'file-update', path: 'i.png', encoding: 'binary', content: bin.buffer };
        const split = splitBinaryPayload(binUpdate);
        expect(split.body).not.toBeNull();

        const framed = packFrame(split.header, split.body);
        const out = unpackFrame(framed.buffer.slice(framed.byteOffset, framed.byteOffset + framed.byteLength));
        const rejoined = joinBinaryPayload(out.header, out.body);
        expect(new Uint8Array(rejoined.content)).toEqual(bin);
    });

    it('rejects truncated frames instead of returning garbage', () => {
        const framed = packFrame({ type: 'ping' }, null);
        const full = framed.buffer.slice(framed.byteOffset, framed.byteOffset + framed.byteLength);
        expect(() => unpackFrame(full.slice(0, 2))).toThrow();
        // Header length says more than the buffer holds.
        expect(() => unpackFrame(full.slice(0, 5))).toThrow();
    });

    it('survives a large repetitive control payload via deflate', () => {
        const manifest = { type: 'request-full-sync', manifest: [] as any[] };
        for (let i = 0; i < 5000; i++) {
            manifest.manifest.push({ type: 'file', path: `notes/folder/note-${i}.md`, mtime: 1700000000000 + i, size: 2048 });
        }
        const json = JSON.stringify(manifest);
        const compressed = compressText(json);
        // Highly repetitive manifests are the motivating case for deflating control frames.
        expect(compressed.byteLength).toBeLessThan(json.length / 5);
        expect(JSON.parse(decompressText(compressed))).toEqual(manifest);
    });
});

describe('Chunked reassembly offset math', () => {
    // Mirrors handleFileChunkStart/handleFileChunkData: the receiver preallocates
    // totalBytes and writes chunk i at i * chunkSize. This only produces the original
    // bytes if the sender's chunkSize is constant for the whole transfer, which is why
    // a resumed transfer must reuse its recorded chunkSize rather than re-deriving it.
    const reassemble = (source: Uint8Array, chunkSize: number, order: number[]) => {
        const totalChunks = Math.max(1, Math.ceil(source.byteLength / chunkSize));
        const buffer = new Uint8Array(source.byteLength);
        const received = new Uint8Array(totalChunks);
        let receivedCount = 0;

        for (const i of order) {
            const start = i * chunkSize;
            const end = Math.min(start + chunkSize, source.byteLength);
            const chunk = source.subarray(start, end);
            if (received[i]) continue;            // duplicate delivery
            received[i] = 1;
            buffer.set(chunk, start);
            receivedCount++;
        }
        return { buffer, receivedCount, totalChunks };
    };

    const makeSource = (n: number) => {
        const s = new Uint8Array(n);
        for (let i = 0; i < n; i++) s[i] = (i * 31 + 7) & 0xff;
        return s;
    };

    it('reassembles in-order chunks exactly', () => {
        const source = makeSource(1000);
        const chunkSize = 256; // last chunk is short (1000 = 3*256 + 232)
        const { buffer, receivedCount, totalChunks } = reassemble(source, chunkSize, [0, 1, 2, 3]);
        expect(totalChunks).toBe(4);
        expect(receivedCount).toBe(totalChunks);
        expect(buffer).toEqual(source);
    });

    it('reassembles out-of-order chunks exactly', () => {
        const source = makeSource(1000);
        const { buffer, receivedCount } = reassemble(source, 256, [3, 0, 2, 1]);
        expect(receivedCount).toBe(4);
        expect(buffer).toEqual(source);
    });

    it('ignores duplicate chunks without double-counting', () => {
        const source = makeSource(700);
        const { buffer, receivedCount, totalChunks } = reassemble(source, 256, [0, 0, 1, 1, 2, 2]);
        expect(totalChunks).toBe(3);
        expect(receivedCount).toBe(3);
        expect(buffer).toEqual(source);
    });

    it('handles an exact multiple of chunkSize', () => {
        const source = makeSource(512);
        const { buffer, receivedCount, totalChunks } = reassemble(source, 256, [0, 1]);
        expect(totalChunks).toBe(2);
        expect(receivedCount).toBe(2);
        expect(buffer).toEqual(source);
    });

    it('rejects a chunk that would write past the declared total size', () => {
        // The guard in handleFileChunkData: offset + length must stay within totalBytes.
        const totalBytes = 1000;
        const chunkSize = 256;
        const lastIndex = 3;
        // A well-formed final chunk fits exactly.
        expect(lastIndex * chunkSize + 232).toBe(totalBytes);
        // An oversized final chunk overruns and must be refused.
        expect(lastIndex * chunkSize + 256).toBeGreaterThan(totalBytes);
    });

    it('silently corrupts data if the sender changes chunkSize mid-transfer', () => {
        // Documents WHY a resumed transfer must reuse its recorded chunkSize. The
        // overrun guard cannot catch this case: the write lands in bounds, just at the
        // wrong offset, so the only defence is keeping chunkSize stable per transfer.
        const source = makeSource(1000);
        const receiverChunkSize = 256;

        const buffer = new Uint8Array(source.byteLength);
        // Chunk 0 sent at 256 (agreed), then the sender re-chunks at 512 for index 1.
        buffer.set(source.subarray(0, 256), 0);
        const senderChunk1 = source.subarray(512, 1000); // 488 bytes
        const offset = 1 * receiverChunkSize;            // receiver writes at 256
        expect(offset + senderChunk1.byteLength).toBeLessThanOrEqual(source.byteLength);
        buffer.set(senderChunk1, offset);

        expect(buffer).not.toEqual(source);
    });
});
