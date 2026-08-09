import { EventEmitter } from 'events';

class MockWebSocket extends EventEmitter {
    readyState: number;
    static OPEN = 1;
    static CONNECTING = 0;
    static CLOSING = 2;
    static CLOSED = 3;

    constructor(url: string) {
        super();
        this.readyState = MockWebSocket.OPEN;
    }

    send(data: any) {
        // mock
    }

    close() {
        this.readyState = MockWebSocket.CLOSED;
        this.emit('close');
    }
}

class MockWebSocketServer extends EventEmitter {
    clients: Set<MockWebSocket>;
    
    constructor(options: any) {
        super();
        this.clients = new Set();
    }

    /**
     * DirectIpServer.start() awaits a 'listening' event before it reports success. Tests run
     * with fake timers, which also fake microtasks, so deferring the event by any means would
     * require the test to advance the clock before the server could finish starting. Firing
     * it the moment the handler is registered keeps the mock deterministic.
     */
    once(event: string | symbol, listener: (...args: any[]) => void): this {
        if (event === 'listening') {
            listener();
            return this;
        }
        return super.once(event, listener);
    }
    
    close() {
        this.emit('close');
    }
}

export default MockWebSocket;
export { MockWebSocketServer as Server, MockWebSocketServer as WebSocketServer, MockWebSocket as WebSocket };
