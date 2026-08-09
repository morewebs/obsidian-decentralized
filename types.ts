// --- Constants ---
export const DISCOVERY_PORT = 41234;
export const DISCOVERY_MULTICAST_ADDRESS = '224.0.0.114';
export const COMPANION_RECONNECT_INTERVAL_MS = 10000;
export const TARGET_CHUNK_TIME_MS = 3000;
export const MIN_CHUNK_SIZE = 64 * 1024;
export const MAX_CHUNK_SIZE = 4 * 1024 * 1024;
export const MAX_BANDWIDTH_SAMPLES = 10;
export const MAX_QUEUE_DEPTH = 50;
export const LOCK_EXPIRATION_MS = 30000;
export const MAX_HASH_CACHE_SIZE = 10000;

// Phase Timeouts
export const REQUESTING_TIMEOUT = 120000;
export const PLANNING_TIMEOUT = 120000;
export const BATCH_TIMEOUT = 300000;
export const COMPLETING_TIMEOUT = 60000;

export enum SyncPhase {
    IDLE = 'IDLE',
    REQUESTING = 'REQUESTING',
    PLANNING = 'PLANNING',
    TRANSFERRING = 'TRANSFERRING',
    COMPLETING = 'COMPLETING',
    ABORTING = 'ABORTING'
}

/** Phase text for the UI. The enum names leaked onto screen as "Phase: TRANSFERRING". */
export function describeSyncPhase(phase: SyncPhase): string {
    switch (phase) {
        case SyncPhase.IDLE: return 'Idle';
        case SyncPhase.REQUESTING: return 'Contacting the other device';
        case SyncPhase.PLANNING: return 'Comparing vaults';
        case SyncPhase.TRANSFERRING: return 'Transferring files';
        case SyncPhase.COMPLETING: return 'Finishing up';
        case SyncPhase.ABORTING: return 'Stopping';
        default: return 'Syncing';
    }
}

export enum SyncErrorCategory {
    CONNECTION_ERROR = 'CONNECTION_ERROR',
    TIMEOUT_ERROR = 'TIMEOUT_ERROR',
    INTEGRITY_ERROR = 'INTEGRITY_ERROR',
    PROTOCOL_ERROR = 'PROTOCOL_ERROR',
    VAULT_ERROR = 'VAULT_ERROR'
}

export class SyncError extends Error {
    constructor(
        public category: SyncErrorCategory,
        message: string,
        public recoverable: boolean,
        public suggestedAction: string
    ) {
        super(message);
        this.name = 'SyncError';
    }
}

export interface AdaptiveSyncConfig {
    maxActiveBatches: number;
    filesPerBatch: number;
    maxBytesPerBatch: number;
}

export interface SyncState {
    isSyncing: boolean;
    currentPhase: SyncPhase;
    peerId: string | null;
    pendingPulls: Set<string>;
    inFlightPulls: Set<string>;
    allowedPulls: Set<string>;
    activeBatches: Map<string, BatchState>;
    activePullBatches: Set<string>;
    phaseStartTime: number;
    phaseTimeoutHandle: number | null;
    missedPings: number;
    filesTotal: number;
    filesTransferred: number;
    bytesTotal: number;
    bytesTransferred: number;
    syncStartTime: number;
    currentFile: string | null;
    currentFileSize: number | null;
    adaptiveConfig: AdaptiveSyncConfig;
    batchStartTimes: Map<string, number>;
}

// --- Type Definitions ---
export type PeerInfo = {
    deviceId: string;
    friendlyName: string;
    ip: string | null;
    port?: number;
    mode?: 'peerjs' | 'direct-ip';
};

export type DiscoveryBeacon = {
    type: 'obsidian-decentralized-beacon';
    peerInfo: PeerInfo;
};

export type FileManifestEntry = {
    path: string;
    mtime: number;
    size: number;
    type: 'file' | 'deleted';
    hash?: string;
    versionVector?: VersionVector;
};

export type FolderManifestEntry = {
    path: string;
    type: 'folder';
};

export type VaultManifest = (FileManifestEntry | FolderManifestEntry)[];

export type DeviceRole = 'primary' | 'secondary';
export type VersionVector = { [deviceId: string]: number };

export type BasePayload = {
    transferId: string;
    messageId?: string;
};

export type HandshakePayload = {
    type: 'handshake';
    peerInfo: PeerInfo;
    pin?: string;
    isResponse?: boolean;
    /** Wire protocol version; see PROTOCOL_VERSION in utils.ts. Absent on 2.x peers. */
    protocolVersion?: number;
};

export type EncryptedFramePayload = {
    type: 'encrypted-frame';
    /** [12B IV][AES-GCM ciphertext of a packFrame buffer] */
    data: ArrayBuffer | Uint8Array;
};

export type RoleAnnouncementPayload = {
    type: 'role-announcement';
    role: DeviceRole;
    deviceId: string;
};

export type ClusterGossipPayload = {
    type: 'cluster-gossip';
    peers: PeerInfo[];
};

export type CompanionPairPayload = {
    type: 'companion-pair';
    peerInfo: PeerInfo;
};

export type AckPayload = {
    type: 'ack';
    transferId: string;
};

export type NackPayload = {
    type: 'nack';
    transferId: string;
    reason: 'integrity-failure' | 'write-error';
};

export type FileUpdatePayload = BasePayload & {
    type: 'file-update';
    path: string;
    content: string | ArrayBuffer;
    mtime: number;
    encoding: 'utf8' | 'binary' | 'base64';
    fileHash?: string;
    compressed?: boolean;
    versionVector?: VersionVector;
};

export type FileDeltaPayload = BasePayload & {
    type: 'file-delta';
    path: string;
    mtime: number;
    patches: string;
    baseHash: string;
    versionVector?: VersionVector;
};

export type FileDeletePayload = BasePayload & {
    type: 'file-delete';
    path: string;
    versionVector?: VersionVector; // Added versionVector for conflict resolution on deletion
};

export type FileRenamePayload = BasePayload & {
    type: 'file-rename';
    oldPath: string;
    newPath: string;
    versionVector?: VersionVector;
};

export type FolderCreatePayload = BasePayload & {
    type: 'folder-create';
    path: string;
};

export type FolderDeletePayload = BasePayload & {
    type: 'folder-delete';
    path: string;
};

export type FolderRenamePayload = BasePayload & {
    type: 'folder-rename';
    oldPath: string;
    newPath: string;
};

// Full Sync Pull-based Payloads
export type FullSyncRequestPayload = {
    type: 'request-full-sync';
    manifest: VaultManifest;
};

export type SyncPlanPayload = {
    type: 'sync-plan';
    filesReceiverWillSend: string[];
    filesInitiatorMustSend: string[];
    filesReceiverMustDelete: string[];
    filesInitiatorMustDelete: string[];
    fileSizes: Record<string, number>;
};

export type RequestBatchPayload = {
    type: 'request-batch';
    paths: string[];
    batchId: string;
};

export type BatchCompletePayload = {
    type: 'batch-complete';
    batchId: string;
    receivedPaths: string[];
    failedPaths: string[];
};

export type FullSyncCompletePayload = {
    type: 'full-sync-complete';
};

export type InitiatorSyncDonePayload = {
    type: 'initiator-sync-done';
};

export type RequestFilePayload = {
    type: 'request-file';
    path: string;
};

export type FileChunkStartPayload = {
    type: 'file-chunk-start';
    path: string;
    mtime: number;
    totalChunks: number;
    transferId: string;
    fileHash: string;
    compressed?: boolean;
    versionVector?: VersionVector;
    /**
     * Total payload size and the sender's chunk size. V3 additions: with these the
     * receiver preallocates one buffer and writes each chunk straight to its final
     * offset, instead of retaining every chunk and then concatenating into a second
     * full-size buffer (which doubled peak memory for the file).
     */
    totalBytes: number;
    chunkSize: number;
};

export type FileChunkDataPayload = {
    type: 'file-chunk-data';
    transferId: string;
    index: number;
    data: ArrayBuffer;
};

export type FileBatchBinaryPayload = {
    type: 'file-batch-binary';
    batchId: string;
    data: ArrayBuffer | Uint8Array;
};

export type PingPayload = {
    type: 'ping';
};

export type PongPayload = {
    type: 'pong';
};

export type SyncPingPayload = {
    type: 'sync-ping';
};

export type SyncPongPayload = {
    type: 'sync-pong';
};

export type ClusterForgetPayload = {
    type: 'cluster-forget';
    targetDeviceId: string;
};

export type ClusterKickPayload = {
    type: 'cluster-kick';
    targetDeviceId: string;
};

export type ClusterRenamePayload = {
    type: 'cluster-rename';
    targetDeviceId: string;
    newName: string;
};

// Locking & Realtime Payloads
export type LockRequestPayload = {
    type: 'lock-request';
    path: string;
    requestId: string;
};

export type LockGrantPayload = {
    type: 'lock-grant';
    path: string;
    requestId: string;
    grantedUntil: number;
};

export type LockDenyPayload = {
    type: 'lock-deny';
    path: string;
    requestId: string;
    reason: string;
};

export type LockReleasePayload = {
    type: 'lock-release';
    path: string;
};

export type EditorActivatePayload = {
    type: 'editor-active';
    path: string;
};

export type EditorDeltaPayload = {
    type: 'editor-delta';
    path: string;
    patches: string;
};

export type SyncAckPayload = {
    type: 'sync-ack';
    messageId: string;
};

// Merkle Tree Payloads
export type MerkleRootPayload = {
    type: 'merkle-root';
    rootHash: string;
};

export type MerkleNodeRequestPayload = {
    type: 'merkle-node-request';
    path: string;
};

export type MerkleNodeResponsePayload = {
    type: 'merkle-node-response';
    path: string;
    children: Record<string, string>;
};

export interface MerkleNode {
    hash: string;
    children?: Record<string, MerkleNode>;
}

export interface TransferStatus {
    id: string;
    path: string;
    direction: 'upload' | 'download';
    peerId: string;
    totalChunks: number;
    processedChunks: number;
    startTime: number;
    lastUpdate: number;
    status: 'active' | 'paused';
    chunkSize?: number;
    compressed?: boolean;
}

export interface FailedSync {
    path: string;
    peerId: string | null;
    timestamp: number;
    type: 'file-update' | 'file-delete' | 'file-delta';
    reason?: string;
    retryCount: number;
}

export interface SyncStatusState {
    text: string;
    icon: string;
    spin?: boolean;
    state: 'loading' | 'success' | 'error' | 'neutral';
}

export type SyncTask =
    | { taskType: 'send-file'; path: string; mtime: number; forceFull: boolean; batchId?: string }
    | { taskType: 'send-file-batch'; paths: string[]; batchId: string }
    | { taskType: 'send-folder-create'; path: string; batchId?: string }
    | { taskType: 'send-delete'; path: string }
    | { taskType: 'send-rename'; oldPath: string; newPath: string };

export interface BatchState {
    peerId: string;
    batchId: string;
    totalCount: number;
    sentCount: number;
    succeededPaths: string[];
    failedPaths: string[];
}

export type SyncData =
    | HandshakePayload
    | EncryptedFramePayload
    | RoleAnnouncementPayload
    | ClusterGossipPayload
    | CompanionPairPayload
    | AckPayload
    | NackPayload
    | FileUpdatePayload
    | FileDeltaPayload
    | FileDeletePayload
    | FileRenamePayload
    | FolderCreatePayload
    | FolderDeletePayload
    | FolderRenamePayload
    | FullSyncRequestPayload
    | SyncPlanPayload
    | RequestBatchPayload
    | BatchCompletePayload
    | FullSyncCompletePayload
    | InitiatorSyncDonePayload
    | RequestFilePayload
    | FileChunkStartPayload
    | FileChunkDataPayload
    | FileBatchBinaryPayload
    | PingPayload
    | PongPayload
    | SyncPingPayload
    | SyncPongPayload
    | ClusterForgetPayload
    | ClusterKickPayload
    | ClusterRenamePayload
    | LockRequestPayload
    | LockGrantPayload
    | LockDenyPayload
    | LockReleasePayload
    | EditorActivatePayload
    | EditorDeltaPayload
    | SyncAckPayload
    | MerkleRootPayload
    | MerkleNodeRequestPayload
    | MerkleNodeResponsePayload;

// Interfaces
export interface PeerServerConfig {
    host: string;
    port: number;
    path: string;
    secure: boolean;
}

export interface DirectIpConfig {
    host: string;
    port: number;
    pin: string;
}

export interface ObsidianDecentralizedSettings {
    syncMode: 'auto' | 'manual' | 'advanced';
    showToasts: boolean;
    deviceId: string;
    friendlyName: string;
    companionPeerId?: string;
    useCustomPeerServer: boolean;
    customPeerServerConfig: PeerServerConfig;
    verboseLogging: boolean;
    connectionMode: 'peerjs' | 'direct-ip';
    directIpHostAddress: string;
    directIpHostPort: number;
    syncAllFileTypes: boolean;
    syncObsidianConfig: boolean;
    conflictResolutionStrategy: 'create-conflict-file' | 'last-write-wins' | 'role-based';
    includedFolders: string;
    excludedFolders: string;
    hideNativeSyncStatus: boolean;
    maximumConcurrentTransfers: number | null;
    chunkSize: number | null;
    debounceDelay: number;
    mtimeTolerance: number;
    knownPeers: PeerInfo[];
    /**
     * Refuse peers we have no encryption key for, and refuse unencrypted messages from peers
     * we do. Off by default because enabling it means re-pairing any device that was paired
     * without a key.
     */
    strictSecurity: boolean;
    idleTimeoutMs: number;
    tombstoneRetentionDays: number;
    enableCompression: boolean;
    enableDeltaSync: boolean;
    deltaSyncThreshold: number;

    // Two-Device Mode Settings
    enableTwoDeviceOptimizations: boolean;
    enableEncryption: boolean;
    enableRealtimeSync: boolean;
    peerKeys: Record<string, string>; // peerId -> base64 PSK
}

export interface TwoDeviceState {
    fileVersions: Record<string, VersionVector>; // path -> { deviceId: version }
    merkleTreeRoot: MerkleNode | null;
}

export const DEFAULT_SETTINGS: ObsidianDecentralizedSettings = {
    syncMode: 'auto',
    showToasts: false,
    deviceId: '',
    friendlyName: 'My New Device',
    companionPeerId: undefined,
    useCustomPeerServer: false,
    customPeerServerConfig: { host: 'localhost', port: 9000, path: '/myapp', secure: false },
    verboseLogging: false,
    connectionMode: 'peerjs',
    directIpHostAddress: '192.168.1.100',
    directIpHostPort: 41235,
    syncAllFileTypes: true,
    syncObsidianConfig: false,
    conflictResolutionStrategy: 'create-conflict-file',
    includedFolders: '',
    excludedFolders: '',
    hideNativeSyncStatus: false,
    maximumConcurrentTransfers: null,
    chunkSize: null,
    debounceDelay: 2000,
    mtimeTolerance: 1500,
    knownPeers: [],
    strictSecurity: false,
    idleTimeoutMs: 30000,
    tombstoneRetentionDays: 30,
    enableCompression: true,
    enableDeltaSync: true,
    deltaSyncThreshold: 50,

    enableTwoDeviceOptimizations: true,
    enableEncryption: true,
    enableRealtimeSync: true,
    peerKeys: {},
};

export interface ILANDiscovery {
    on(event: string, listener: (...args: any[]) => void): this;
    off(event: string, listener: (...args: any[]) => void): this;
    startBroadcasting(peerInfo: PeerInfo): void;
    stopBroadcasting(): void;
    startListening(): void;
    stop(): void;
    getDiscoveredPeers(): PeerInfo[];
}
