jest.mock('ws');
jest.mock('obsidian');

Object.defineProperty(global, 'window', {
    value: {
        setTimeout, clearTimeout, setInterval, clearInterval,
        addEventListener: jest.fn(), removeEventListener: jest.fn()
    },
    writable: true
});

import { DirectIpServer, DirectIpClient } from './directip';

// Pull the mock class out of the mocked module rather than importing __mocks__/ws directly.
// Loading that file as its own module as well as through jest.mock('ws') registers it twice,
// and the duplicate registration makes DirectIpServer's dynamic `import('ws')` never settle.
const MockWebSocket: any = jest.requireMock('ws').WebSocket;
(global as any).WebSocket = MockWebSocket;

describe('DirectIpServer', () => {
    let server: DirectIpServer;
    let mockPlugin: any;


    beforeEach(async () => {
        // queueMicrotask must stay real: the ws mock uses it to signal that the server is
        // bound, and DirectIpServer.start() awaits that before resolving.
        jest.useFakeTimers({ doNotFake: ['queueMicrotask'] });
        mockPlugin = {
            log: jest.fn(),
            showNotice: jest.fn(),
            updateStatus: jest.fn(),
            settings: { deviceId: 'test-device' }
        };
        server = new DirectIpServer(mockPlugin, 8080, '1234');
        // start() is async now — it dynamically imports 'ws' and waits for the socket to
        // actually bind — so wait for it before inspecting the server.
        await server.listening;
    });

    afterEach(() => {
        server.stop();
        jest.useRealTimers();
    });

    test('should reap stale clients', () => {
        // server is started in constructor
        
        // Add a mock client manually
        const mockSocket = new MockWebSocket('ws://localhost');
        const closeSpy = jest.spyOn(mockSocket, 'close');
        
        (server as any).clients.set('device-1', {
            socket: mockSocket,
            peerId: 'device-1',
            lastHeard: Date.now() - 25000 // 25 seconds ago (stale)
        });

        expect((server as any).clients.size).toBe(1);

        // Advance timers to trigger the reapInterval
        jest.advanceTimersByTime(30000); 

        // The socket should be closed and removed from the map
        expect(closeSpy).toHaveBeenCalled();
        expect((server as any).clients.size).toBe(0);
    });

    test('should not reap active clients', () => {
        // server is started in constructor
        
        const mockSocket = new MockWebSocket('ws://localhost');
        const closeSpy = jest.spyOn(mockSocket, 'close');
        
        (server as any).clients.set('device-1', {
            socket: mockSocket,
            peerId: 'device-1',
            lastHeard: Date.now() - 5000 // 5 seconds ago (active)
        });

        jest.advanceTimersByTime(10000); 

        expect(closeSpy).not.toHaveBeenCalled();
        expect((server as any).clients.size).toBe(1);
    });

    test('should teardown cleanly on stop()', () => {
        // server is started in constructor
        
        const mockSocket = new MockWebSocket('ws://localhost');
        const closeSpy = jest.spyOn(mockSocket, 'close');
        
        (server as any).clients.set('device-1', {
            socket: mockSocket,
            peerId: 'device-1',
            lastHeard: Date.now()
        });

        expect((server as any).reapInterval).not.toBeNull();
        expect((server as any).wss).not.toBeNull();

        server.stop();
        // Since stop clears interval, let's just make sure interval is null
        expect((server as any).reapInterval).toBeNull();
        expect((server as any).wss).toBeNull();
        expect(closeSpy).toHaveBeenCalled();
        expect((server as any).clients.size).toBe(0);
    });
});

describe('DirectIpClient', () => {
    let client: DirectIpClient;
    let mockPlugin: any;

    beforeEach(() => {
        jest.useFakeTimers();
        mockPlugin = {
            log: jest.fn(),
            showNotice: jest.fn(),
            updateStatus: jest.fn(),
            getMyPeerInfo: jest.fn().mockReturnValue({ deviceId: 'test-device' }),
            rejectPendingAck: jest.fn(),
            settings: { deviceId: 'test-device' }
        };
        client = new DirectIpClient(mockPlugin, { host: 'localhost', port: 1234, pin: '1234' } as any);
    });

    afterEach(() => {
        client.stop();
        jest.useRealTimers();
    });

    test('send() should cap the buffer at 100 items', () => {
        // This is about what happens while the link is down, so state that precondition
        // rather than relying on whether the mock socket happens to look open.
        (client as any).ws = null;

        for (let i = 0; i < 150; i++) {
            client.send({ type: 'test', index: i }).catch(() => {});
        }

        const buffer = (client as any).sendBuffer;
        expect(buffer.length).toBe(100);
        
        // Oldest 50 should be dropped. The 0th element should be index 50.
        expect(buffer[0].data.index).toBe(50);
        expect(buffer[99].data.index).toBe(149);
    });

    test('flushSendBuffer should drop item after max retries', () => {
        const mockSocket = new MockWebSocket('ws://localhost');
        mockSocket.send = jest.fn(() => { throw new Error('Send failed'); });
        mockSocket.readyState = 1;
        (client as any).ws = mockSocket;

        client.send({ type: 'test', data: 'hello' }).catch(() => {});
        
        const buffer = (client as any).sendBuffer;
        expect(buffer.length).toBe(1);
        expect(buffer[0].retries).toBe(1);
        
        // Manual flushes to trigger retries
        (client as any).flushSendBuffer();
        expect(buffer[0].retries).toBe(2);

        (client as any).flushSendBuffer();
        // The 3rd retry drops it
        expect(buffer.length).toBe(0);
    });
});
