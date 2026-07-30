import { Platform } from 'obsidian';
import { DirectIpConfig, SyncData } from './types';
import { splitBinaryPayload, joinBinaryPayload, packFrame, unpackFrame } from './utils';
import type ObsidianDecentralizedPlugin from './main';

// Heartbeat constants (mirror main.ts startHeartbeat)
const HEARTBEAT_INTERVAL_MS = 5000;   // ping every 5 s
const LIVENESS_TIMEOUT_MS   = 20000;  // declare dead after 20 s of silence

/**
 * Frame a message for the wire. Messages with a bulk binary body (file chunks,
 * binary batches, binary file updates, encrypted frames) are sent as a single
 * binary frame; everything else goes as JSON text.
 *
 * The per-type branches this used to carry are now one call to splitBinaryPayload,
 * which is shared with the encryption layer in main.ts so both agree on which
 * field holds the body.
 */
function encodeMessage(msg: SyncData | any): string | ArrayBuffer {
    const { header, body } = splitBinaryPayload(msg);
    if (!body) return JSON.stringify(msg);
    const framed = packFrame(header, body);
    return framed.buffer.slice(framed.byteOffset, framed.byteOffset + framed.byteLength);
}

function decodeMessage(data: string | ArrayBuffer | Uint8Array): any {
    if (typeof data === 'string') {
        return JSON.parse(data);
    }
    let buffer: ArrayBuffer;
    if (data instanceof ArrayBuffer) {
        buffer = data;
    } else if (data instanceof Uint8Array || (typeof Buffer !== 'undefined' && Buffer.isBuffer(data))) {
        buffer = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
    } else {
        throw new Error('Unsupported data type');
    }

    const { header, body } = unpackFrame(buffer);
    return joinBinaryPayload(header, body);
}

// ─── DirectIpServer ────────────────────────────────────────────────────────────

interface ServerClientEntry {
    socket: any;
    lastHeard: number;
}

export class DirectIpServer {
    private wss: any | null = null;
    /** deviceId → {socket, lastHeard} */
    private clients: Map<string, ServerClientEntry> = new Map();
    private pin: string;
    private reapInterval: number | null = null;

    constructor(private plugin: ObsidianDecentralizedPlugin, port: number, pin: string) {
        this.pin = pin;
        if (Platform.isMobile) {
            this.plugin.showNotice("Offline Host mode is only available on Desktop.", 'important');
            return;
        }
        this.start(port);
    }

    private start(port: number) {
        try {
            const { WebSocketServer } = require('ws');
            this.wss = new WebSocketServer({ port });
        } catch (err: any) {
            this.plugin.log("Failed to load/initialize 'ws' module:", err);
            this.plugin.showNotice(`Failed to start Offline Host server: ${err.message || err}`, 'error');
            this.wss = null;
            return;
        }
        
        this.wss.on('connection', (socket: any, request: any) => {
            const url = new URL(request.url || '', `http://${request.headers.host || 'localhost'}`);
            const pin = url.searchParams.get('pin');
            const deviceId = url.searchParams.get('deviceId') || 'unknown';

            if (pin !== this.pin) {
                socket.close(1008, 'Invalid PIN');
                return;
            }

            const entry: ServerClientEntry = { socket, lastHeard: Date.now() };
            this.clients.set(deviceId, entry);

            socket.on('message', (data: any, isBinary: boolean) => {
                // Update liveness timestamp on every message from this client
                const e = this.clients.get(deviceId);
                if (e) e.lastHeard = Date.now();

                try {
                    let parsedData: any;
                    let parsedSuccessfully = false;
                    // IMPORTANT: 'ws' delivers TEXT frames as a Node Buffer too, and Buffer IS
                    // an instanceof Uint8Array — so the frame kind must be decided by the
                    // isBinary flag alone. Checking `data instanceof Uint8Array` here routed
                    // every JSON text message into the binary decoder, which always threw,
                    // silently dropping ALL client→server traffic (handshakes included).
                    if (isBinary) {
                        try {
                            parsedData = decodeMessage(data);
                            parsedSuccessfully = true;
                        } catch (decodeErr) {
                            this.plugin.log(`Server: decodeMessage failed for binary message from ${deviceId}:`, decodeErr);
                        }
                    } else {
                        try {
                            parsedData = JSON.parse(data.toString());
                            parsedSuccessfully = true;
                        } catch (jsonErr) {
                            this.plugin.log(`Server: JSON parse failed for string message from ${deviceId}:`, jsonErr);
                        }
                    }

                    if (parsedSuccessfully) {
                        const mockConn = {
                            send: (msg: any) => this.sendTo(deviceId, msg),
                            peer: deviceId,
                            open: true,
                            // main.ts (heartbeat, PIN rejection) calls conn.close(); without
                            // this method those call sites threw TypeError every tick.
                            close: () => {
                                try { socket.close(); } catch (_) { /* ignore */ }
                                if (this.clients.get(deviceId)?.socket === socket) {
                                    this.clients.delete(deviceId);
                                }
                                this.plugin.connections?.delete(deviceId);
                                this.plugin.updateStatus();
                            },
                        } as any;

                        this.plugin.handleRawIncomingData(parsedData, mockConn).catch((e: any) => {
                            this.plugin.log(`Server: Failed to handle raw incoming data from ${deviceId}:`, e);
                            this.plugin.showNotice(`Error processing received sync message from ${deviceId}.`, 'error');
                        });
                    }
                } catch (e) {
                    this.plugin.log('Error parsing WS message', e);
                }
            });

            socket.on('close', () => {
                // Only remove the registration if it still belongs to THIS socket.
                // A stale socket's late close event must not evict a client that
                // has already reconnected with a fresh socket under the same deviceId.
                if (this.clients.get(deviceId)?.socket === socket) {
                    this.clients.delete(deviceId);
                    this.plugin.connections?.delete(deviceId);
                }
                this.plugin.updateStatus();
            });
            
            socket.on('error', (err: any) => {
                this.plugin.log(`WS Client Error (${deviceId}):`, err);
            });
        });

        this.wss.on('error', (err: Error) => {
            this.plugin.showNotice(`Offline server error: ${err.message}`, 'error');
            this.plugin.log("Offline Server Error:", err);
            this.stop();
        });

        // Stale-client reaper: terminate clients that haven't sent anything
        // within the liveness window (mirrors client-side heartbeat timeout).
        this.reapInterval = setInterval(() => {
            const now = Date.now();
            const toReap: string[] = [];
            for (const [deviceId, entry] of this.clients.entries()) {
                if (now - entry.lastHeard > LIVENESS_TIMEOUT_MS) {
                    toReap.push(deviceId);
                }
            }
            for (const deviceId of toReap) {
                const entry = this.clients.get(deviceId);
                if (entry) {
                    this.plugin.log(`Server: reaping stale client ${deviceId} (silent for ${Math.round((now - entry.lastHeard) / 1000)}s)`);
                    try { entry.socket.close(); } catch (_) { /* ignore */ }
                    this.clients.delete(deviceId);
                    this.plugin.connections?.delete(deviceId);
                    this.plugin.updateStatus();
                }
            }
        }, LIVENESS_TIMEOUT_MS) as any as number;

        this.plugin.log(`Offline WebSocket server listening on port ${port}`);
    }

    getClients(): string[] {
        return Array.from(this.clients.keys());
    }

    sendTo(peerId: string, data: any) {
        const entry = this.clients.get(peerId);
        if (entry && entry.socket.readyState === 1 /* OPEN */) {
            try {
                const encoded = encodeMessage(data);
                entry.socket.send(encoded);
            } catch (err) {
                this.plugin.log(`DirectIpServer: Failed to send message to peer ${peerId}:`, err);
            }
        }
    }

    hasClient(peerId: string): boolean {
        return this.clients.has(peerId);
    }

    getBufferedAmount(peerId: string): number {
        const entry = this.clients.get(peerId);
        if (!entry) return 0;
        return entry.socket._socket ? entry.socket._socket.bufferSize : entry.socket.bufferedAmount;
    }

    send(data: any, excludePeerId?: string) {
        let encoded: any;
        try {
            encoded = encodeMessage(data);
        } catch (err) {
            this.plugin.log('DirectIpServer: Failed to encode message for broadcast:', err);
            return;
        }
        for (const [deviceId, entry] of this.clients.entries()) {
            if (deviceId === excludePeerId) continue;
            if (entry.socket.readyState === 1 /* OPEN */) {
                try {
                    const finalEncoded = encoded instanceof ArrayBuffer ? encoded.slice(0) : encoded;
                    entry.socket.send(finalEncoded);
                } catch (err) {
                    this.plugin.log('DirectIpServer: Broadcast send failed for client:', err);
                }
            }
        }
    }

    stop() {
        if (this.reapInterval !== null) {
            clearInterval(this.reapInterval);
            this.reapInterval = null;
        }
        for (const entry of this.clients.values()) {
            try { entry.socket.close(); } catch (_) { /* ignore */ }
        }
        this.clients.clear();
        if (this.wss) {
            this.wss.close();
        }
        this.wss = null;
        this.plugin.log("Offline Server stopped.");
    }
}

// ─── DirectIpClient ────────────────────────────────────────────────────────────

export class DirectIpClient {
    /** True once the socket is open AND at least one message has been received. */
    public isLive: boolean = false;
    /** True when the socket is OPEN (TCP layer), independent of liveness. */
    public isOpen: boolean = false;
    /** Set when a fatal, non-retriable error has occurred (e.g. PIN rejection). */
    public isFatalError: boolean = false;

    private ws: WebSocket | null = null;
    private sendBuffer: { data: any, retries: number, resolve?: () => void, reject?: (e: any) => void }[] = [];
    private isStopped = false;

    // Reconnect backoff state
    private reconnectAttempts = 0;
    private reconnectTimeout: number | null = null;

    // Heartbeat / keep-alive state
    private heartbeatInterval: number | null = null;
    private lastHeardAt: number = 0;

    constructor(private plugin: ObsidianDecentralizedPlugin, private config: DirectIpConfig) {
        this.connect();
    }
    
    getBufferedAmount(): number {
        return this.ws ? this.ws.bufferedAmount : 0;
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    private stopHeartbeat() {
        if (this.heartbeatInterval !== null) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }
    }

    /** Reject and drop every queued send. Without this, callers awaiting send()
     *  hang forever when the connection never (re)opens. */
    private drainSendBuffer(reason: string) {
        const pending = this.sendBuffer;
        this.sendBuffer = [];
        for (const item of pending) {
            if (item.data?.transferId) {
                this.plugin.rejectPendingAck(item.data.transferId, reason);
            }
            item.reject?.(new Error(reason));
        }
    }

    private startHeartbeat() {
        this.stopHeartbeat();
        this.lastHeardAt = Date.now(); // socket just opened — reset the clock
        this.heartbeatInterval = window.setInterval(() => {
            if (!this.ws || this.ws.readyState !== 1 /* WebSocket.OPEN */) {
                this.stopHeartbeat();
                return;
            }

            // Send a ping to the server
            try {
                this.ws.send(encodeMessage({ type: 'ping' }));
            } catch (e) {
                this.plugin.log('DirectIpClient: failed to send heartbeat ping', e);
            }

            // Check liveness window — if exceeded, force-close to trigger reconnect
            if (Date.now() - this.lastHeardAt > LIVENESS_TIMEOUT_MS) {
                this.plugin.log(`DirectIpClient: host silent for >${LIVENESS_TIMEOUT_MS / 1000}s — force-closing socket`);
                this.stopHeartbeat();
                this.ws?.close();
            }
        }, HEARTBEAT_INTERVAL_MS);
    }

    /**
     * Compute the next backoff delay, update status to "reconnecting", and
     * schedule a call to connect().
     */
    private scheduleReconnect() {
        if (this.isStopped) return;

        this.reconnectAttempts++;
        const backoff = Math.min(30000, this.reconnectAttempts * 2000);

        this.plugin.log(`DirectIpClient: reconnect attempt ${this.reconnectAttempts} in ${backoff / 1000}s`);
        this.plugin.updateStatus({
            text: `Reconnecting to host… (${this.reconnectAttempts})`,
            icon: 'refresh-cw',
            spin: true,
            state: 'loading',
        });

        if (this.reconnectTimeout !== null) {
            clearTimeout(this.reconnectTimeout);
        }
        this.reconnectTimeout = window.setTimeout(() => {
            this.reconnectTimeout = null;
            this.connect();
        }, backoff);
    }

    private connect() {
        if (this.isStopped) return;
        this.isFatalError = false;
        
        const wsUrl = `ws://${this.config.host}:${this.config.port}/?pin=${encodeURIComponent(this.config.pin)}&deviceId=${encodeURIComponent(this.plugin.settings.deviceId)}`;
        this.ws = new WebSocket(wsUrl);
        this.ws.binaryType = 'arraybuffer';

        this.ws.onopen = () => {
            // Cancel any pending reconnect timer
            if (this.reconnectTimeout !== null) {
                clearTimeout(this.reconnectTimeout);
                this.reconnectTimeout = null;
            }
            // Reset backoff counter
            this.reconnectAttempts = 0;

            this.isOpen = true;
            // isLive remains false until the first incoming message proves the
            // far-end is processing traffic (Phase 4 accuracy requirement).

            this.plugin.showNotice(`Connected to Offline Host at ${this.config.host}`, 'important', 3000);

            // Emit "connecting" until first message confirms liveness
            this.plugin.updateStatus({
                text: 'Connected — verifying link…',
                icon: 'plug',
                spin: true,
                state: 'loading',
            });

            this.startHeartbeat();
            this.flushSendBuffer();
        };

        this.ws.onmessage = (event) => {
            // Every incoming message proves the far-end is alive
            this.lastHeardAt = Date.now();
            if (!this.isLive) {
                this.isLive = true;
                // First confirmed live message — emit proper connected status
                this.plugin.updateStatus();
            }

            try {
                let parsedData: any;
                let parsedSuccessfully = false;

                if (typeof event.data === 'string') {
                    try {
                        parsedData = JSON.parse(event.data);
                        parsedSuccessfully = true;
                    } catch (jsonErr) {
                        this.plugin.log('DirectIpClient: JSON parse failed for string message:', jsonErr);
                    }
                } else if (event.data instanceof ArrayBuffer) {
                    try {
                        parsedData = decodeMessage(event.data);
                        parsedSuccessfully = true;
                    } catch (decodeErr) {
                        this.plugin.log('DirectIpClient: decodeMessage failed for ArrayBuffer:', decodeErr);
                    }
                } else {
                    this.plugin.log('DirectIpClient: Unsupported WebSocket message type dropped:', typeof event.data);
                }
                
                if (parsedSuccessfully) {
                    const mockConn = {
                        send: (data: any) => this.send(data),
                        peer: 'direct-ip-host',
                        open: true,
                        // main.ts heartbeat calls conn.close() on silent peers; map it to a
                        // reconnect cycle instead of throwing TypeError.
                        close: () => this.triggerReconnect()
                    } as any;

                    this.plugin.handleRawIncomingData(parsedData, mockConn).catch((e: any) => {
                        this.plugin.log('Client: Failed to handle raw incoming data:', e);
                        this.plugin.showNotice('Error processing received sync message.', 'error');
                    });
                }
            } catch (e) {
                this.plugin.log('Offline WS Parse Error:', e);
            }
        };

        this.ws.onclose = (event) => {
            this.isOpen = false;
            this.isLive = false;
            this.stopHeartbeat();

            // Intentional shutdown — do nothing
            if (this.isStopped) return;

            // Fatal: PIN / auth rejection → no retry, surface error
            if (event.code === 1008) {
                this.isFatalError = true;
                this.drainSendBuffer('Connection rejected by host (invalid PIN)');
                this.plugin.log(`DirectIpClient: fatal close (1008 PIN/auth rejection)`);
                this.plugin.showNotice('Connection rejected by host: invalid PIN.', 'error');
                this.plugin.updateStatus({
                    text: 'Host rejected PIN',
                    icon: 'shield-off',
                    state: 'error',
                });
                return;
            }

            // All other closes — schedule exponential backoff reconnect
            this.scheduleReconnect();
        };
        
        this.ws.onerror = (err) => {
            // onclose always fires after onerror, so reconnect logic lives there.
            this.plugin.log('Offline WS Error:', err);
        };
    }

    // ── Public API ─────────────────────────────────────────────────────────────

    send(data: any): Promise<void> {
        return new Promise((resolve, reject) => {
            if (this.sendBuffer.length >= 100) {
                const dropped = this.sendBuffer.shift(); // Drop oldest message
                if (dropped?.data?.transferId) {
                    this.plugin.rejectPendingAck(dropped.data.transferId, 'Buffer overflow');
                }
                dropped?.reject?.(new Error('Buffer overflow'));
            }
            this.sendBuffer.push({ data, retries: 0, resolve, reject });
            this.flushSendBuffer();
        });
    }

    private flushSendBuffer() {
        if (!this.ws || this.ws.readyState !== 1 /* WebSocket.OPEN */) return;
        
        while (this.sendBuffer.length > 0) {
            const item = this.sendBuffer[0];
            const data = item.data;
            let encoded;
            try {
                encoded = encodeMessage(data);
            } catch (e: any) {
                this.plugin.log('DirectIpClient: Failed to encode message, dropping it.', e);
                this.plugin.showNotice(`Failed to encode sync message: ${e.message}`, 'important');
                if (data.transferId) {
                    this.plugin.rejectPendingAck(data.transferId, `Encode failed: ${e.message}`);
                }
                item.reject?.(e);
                this.sendBuffer.shift();
                continue;
            }
            try {
                this.ws.send(encoded);
                item.resolve?.();
                this.sendBuffer.shift();
            } catch (e) {
                item.retries++;
                if (item.retries < 3) {
                    setTimeout(() => this.flushSendBuffer(), 1000);
                } else {
                    this.plugin.log('DirectIpClient: Dropping message after max retries on send failure');
                    item.reject?.(e);
                    this.sendBuffer.shift();
                }
                break;
            }
        }
    }

    /**
     * Force-close the current socket and immediately begin the backoff-reconnect
     * cycle.  Called by the network-change handler in main.ts (Phase 3.2).
     */
    public triggerReconnect() {
        if (this.isStopped || this.isFatalError) return;
        this.stopHeartbeat();
        if (this.ws && this.ws.readyState !== 3) {
            this.ws.close(); // let onclose trigger scheduleReconnect
        } else {
            this.scheduleReconnect();
        }
    }

    startPolling() {
        // Legacy compat, no-op for WebSockets
    }

    stop() {
        this.isStopped = true;
        this.isOpen = false;
        this.isLive = false;
        this.stopHeartbeat();
        this.drainSendBuffer('Client stopped');

        if (this.reconnectTimeout !== null) {
            clearTimeout(this.reconnectTimeout);
            this.reconnectTimeout = null;
        }

        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
        this.plugin.showNotice("Disconnected from Offline Host.", 'important', 3000);
    }
}
