import { Notice, Plugin, TFile, TFolder, TAbstractFile, Platform, debounce, MarkdownView, setIcon } from 'obsidian';
import Peer, { DataConnection, PeerJSOption } from 'peerjs';
import DiffMatchPatch from 'diff-match-patch';

// UI imports
import { ConnectionModal, SelectPeerModal, ConflictCenter, SyncProgressModal, formatBytes } from './ui';

// Settings tab import
import { ObsidianDecentralizedSettingTab } from './settings-tab';

// LAN Discovery imports
import { DummyLANDiscovery, DesktopLANDiscovery } from './discovery';

// Direct IP imports
import { DirectIpServer, DirectIpClient } from './directip';

// Types & Constants imports
import {
    COMPANION_RECONNECT_INTERVAL_MS,
    TARGET_CHUNK_TIME_MS,
    MIN_CHUNK_SIZE,
    MAX_CHUNK_SIZE,
    MAX_BANDWIDTH_SAMPLES,
    LOCK_EXPIRATION_MS,
    MAX_HASH_CACHE_SIZE,
    REQUESTING_TIMEOUT,
    PLANNING_TIMEOUT,
    BATCH_TIMEOUT,
    COMPLETING_TIMEOUT,
    SyncPhase,
    SyncErrorCategory,
    SyncError,
    SyncState,
    PeerInfo,
    VaultManifest,
    DeviceRole,
    VersionVector,
    HandshakePayload,
    ClusterGossipPayload,
    CompanionPairPayload,
    FileUpdatePayload,
    FileDeltaPayload,
    FileDeletePayload,
    FileRenamePayload,
    FolderCreatePayload,
    FolderDeletePayload,
    FolderRenamePayload,
    FullSyncRequestPayload,
    SyncPlanPayload,
    RequestBatchPayload,
    BatchCompletePayload,
    RequestFilePayload,
    FileChunkStartPayload,
    FileChunkDataPayload,
    ClusterForgetPayload,
    ClusterKickPayload,
    ClusterRenamePayload,
    LockRequestPayload,
    LockGrantPayload,
    LockDenyPayload,
    LockReleasePayload,
    EditorActivatePayload,
    EditorDeltaPayload,
    MerkleRootPayload,
    MerkleNodeRequestPayload,
    MerkleNodeResponsePayload,
    MerkleNode,
    TransferStatus,
    FailedSync,
    SyncStatusState,
    SyncTask,
    BatchState,
    SyncData,
    DirectIpConfig,
    ObsidianDecentralizedSettings,
    TwoDeviceState,
    DEFAULT_SETTINGS,
    ILANDiscovery,
    FileBatchBinaryPayload
} from './types';

// Utils imports
import {
    compressText,
    decompressText,
    arrayBufferToBase64,
    base64ToArrayBuffer,
    packFilesToTLV,
    unpackTLVToFiles,
    PackedFile,
    PROTOCOL_VERSION,
    splitBinaryPayload,
    joinBinaryPayload,
    packFrame,
    unpackFrame
} from './utils';

import { TimeoutManager } from './src/utils/Timeouts';
import { QueueManager } from './src/core/QueueManager';
import { ConnectionManager } from './src/core/ConnectionManager';

export default class ObsidianDecentralizedPlugin extends Plugin {
    settings: ObsidianDecentralizedSettings;
    peer: Peer | null = null;
    connections: Map<string, DataConnection> = new Map();
    clusterPeers: Map<string, PeerInfo> = new Map();
    lanDiscovery: ILANDiscovery;
    private fileLocks: Map<string, Promise<void>> = new Map();

    // Architectural Managers
    public timeoutManager: TimeoutManager;
    public queueManager: QueueManager;
    public connectionManager: ConnectionManager;


    public syncState: SyncState = {
        isSyncing: false,
        currentPhase: SyncPhase.IDLE,
        peerId: null,
        pendingPulls: new Set(),
        allowedPulls: new Set(),
        activeBatches: new Map(),
        phaseStartTime: 0,
        phaseTimeoutHandle: null,
        missedPings: 0,
        filesTotal: 0,
        filesTransferred: 0,
        bytesTotal: 0,
        bytesTransferred: 0,
        syncStartTime: 0,
        currentFile: null,
        currentFileSize: null,
        inFlightPulls: new Set(),
        activePullBatches: new Set(),
        adaptiveConfig: {
            maxActiveBatches: 1,
            filesPerBatch: 50,
            maxBytesPerBatch: 50 * 1024 * 1024
        },
        batchStartTimes: new Map()
    };
    private pendingSyncAcks: Map<string, { resolve: () => void, reject: (e: Error) => void }> = new Map();
    private lastSuccessfulMessageTime: Map<string, number> = new Map();
    // Receiver-side dedup of retried sync control messages (insertion-ordered, capped)
    private processedMessageIds: Set<string> = new Set();

    private ignoreEvents: Map<string, number> = new Map();
    private statusBar: HTMLElement;
    private conflictCenter: ConflictCenter;
    public activeTransfers: Map<string, TransferStatus> = new Map();
    public joinPin: string | null = null;
    public activePsk: string | null = null;
    private clusterConnectionInterval: number | null = null;
    public pendingConnections: Set<string> = new Set();
    private pendingFileChunks: Map<string, { path: string, mtime: number, chunks: ArrayBuffer[], total: number, receivedCount: number, lastUpdated: number, fileHash: string, compressed?: boolean, versionVector?: VersionVector }> = new Map();
    
    // Timeouts and Keep-alives
    private syncIdleTimeout: number | null = null;
    private syncKeepAliveInterval: number | null = null;
    private pendingAcks: Map<string, { resolve: () => void, reject: (e: Error) => void, peerId: string }> = new Map();
    private lastStatusUpdate: number = 0;
    private currentConcurrency = 16;
    private currentChunkSize = 512 * 1024;
    private targetChunkSize = 512 * 1024;
    private successfulTransfersSinceLastIncrease = 0;
    private peerInitRetryTimeout: number | null = null;
    private peerInitAttempts = 0;
    private debouncedHandleFileChange: (file: TAbstractFile) => void;
    public directIpServer: DirectIpServer | null = null;
    public directIpClient: DirectIpClient | null = null;
    private lastHeard: Map<string, number> = new Map();

    // Network-change handling (Phase 3.2)
    private networkChangeHandler: (() => void) | null = null;
    private peerReconnectFallbackTimeout: number | null = null;
    private statePath: string;
    public manualPingStart: Map<string, number> = new Map();
    private debouncedSaveState: () => void;
    public failedSyncs: FailedSync[] = [];
    private syncedHashes: Map<string, { hash: string, timestamp: number }> = new Map();

    // Bandwidth measurement & delta sync states
    private recentTransferSamples: { bytes: number, durationMs: number }[] = [];
    private currentBandwidthEstimate: number = 0;
    private lastSentContent: Map<string, { content: string, timestamp: number }> = new Map();

    // Two-Device Mode State
    public currentRole: DeviceRole | null = null;
    public twoDeviceState: TwoDeviceState = { fileVersions: {}, merkleTreeRoot: null };
    public tombstones: Record<string, number> = {};
    public currentSyncIsTwoDeviceMode: boolean | null = null;
    private syncDrainCallback: (() => void) | null = null;
    
    // Pull-based Sync State
    private pullRetries: Map<string, number> = new Map();
    private peerFileSizes: Record<string, number> = {};
    private localSyncComplete: Map<string, boolean> = new Map();
    private peerSyncComplete: Map<string, boolean> = new Map();
    // Cached parsed folder filters — invalidated on settings change to avoid re-parsing on every vault event
    private _cachedExcludedFolders: string[] | null = null;
    private _cachedIncludedFolders: string[] | null = null;
    
    // File Locking State
    public heldLocks: Map<string, { peerId: string, expiresAt: number }> = new Map();
    public remoteLocks: Map<string, { peerId: string, expiresAt: number }> = new Map();
    private pendingLockRequests: Map<string, { resolve: (granted: boolean) => void, timeout: number }> = new Map();
    
    // Real-time Editor Sync State
    public activeEditorLocks: Map<string, string> = new Map();
    private isApplyingRemoteEdit: boolean = false;
    private debouncedEditorChange: (editor: any, info: any) => void;

    async onload() {
        // Initialize Core Managers
        this.timeoutManager = new TimeoutManager();
        this.connectionManager = new ConnectionManager(this.timeoutManager);
        this.queueManager = new QueueManager(this.timeoutManager, async (item) => {
            try {
                await this.processQueueItem(item);
                return true;
            } catch (e) {
                return false;
            }
        });

        if (Platform.isMobile) {
            this.lanDiscovery = new DummyLANDiscovery();
        } else {
            this.lanDiscovery = new DesktopLANDiscovery();
        }

        this.statePath = `${this.manifest.dir}/state.json`;
        this.debouncedSaveState = debounce(this.saveState.bind(this), 1000);
        this.debouncedEditorChange = debounce(this.handleEditorChangeDebounced.bind(this), 200);

        await this.loadSettings();
        this.applyHideNativeSync();
        this.statusBar = this.addStatusBarItem();
        this.addSettingTab(new ObsidianDecentralizedSettingTab(this.app, this));
        this.conflictCenter = new ConflictCenter(this.app, this);
        this.conflictCenter.registerRibbon();
        this.addRibbonIcon('users', 'Connect to a Peer', () => new ConnectionModal(this.app, this).open());
        this.addRibbonIcon('refresh-cw', 'Force Full Sync with Peer', () => {
            if (this.connections.size === 0) { this.showNotice("No peers connected.", 'info'); return; }
            new SelectPeerModal(this.app, this.connections, this.clusterPeers, (peerId: string) => this.requestFullSyncFromPeer(peerId)).open();
        });

        this.debouncedHandleFileChange = debounce(this.handleFileChange.bind(this), this.settings.debounceDelay);
        this.registerEvent(this.app.vault.on('create', (file) => this.handleEvent(file)));
        this.registerEvent(this.app.vault.on('modify', (file) => this.handleEvent(file)));
        this.registerEvent(this.app.vault.on('delete', (file) => this.handleEvent(file)));
        this.registerEvent(this.app.vault.on('rename', (file, oldPath) => this.handleRenameEvent(file, oldPath)));
        this.registerEvent(this.app.workspace.on('editor-change', (editor, info) => this.handleEditorChange(editor, info)));

        this.initializeConnectionManager();
        this.startHeartbeat();
        this.registerInterval(window.setInterval(() => this.cleanupPendingChunks(), 60000));
        this.registerInterval(window.setInterval(() => this.retryFailedSyncs(), 60000));
        this.registerInterval(window.setInterval(() => this.cleanupLocks(), 5000));

        // Centralized network-change listeners (Phase 3.2)
        this.networkChangeHandler = () => this.handleNetworkChange();
        window.addEventListener('online',  this.networkChangeHandler);
        window.addEventListener('offline', this.networkChangeHandler);
        this.lanDiscovery.on('network-change', this.networkChangeHandler);
        
        await this.loadState();
        this.pruneTombstones();
    }

    onunload() {
        // Remove centralized network-change listeners
        if (this.networkChangeHandler) {
            window.removeEventListener('online',  this.networkChangeHandler);
            window.removeEventListener('offline', this.networkChangeHandler);
            this.lanDiscovery.off('network-change', this.networkChangeHandler);
            this.networkChangeHandler = null;
        }

        this.peer?.destroy();
        this.lanDiscovery.stop();
        this.directIpServer?.stop();
        this.directIpClient?.stop();

        this.activeTransfers.clear();
        this.connections.clear(); 
        
        // Safely destroy all background timeouts and queue processes
        this.queueManager.clear();
        this.timeoutManager.clearAll();

        this.saveState(); // Force immediate save on unload instead of debounced delay
    }

    // --- Core Two-Device Infrastructure ---
    isTwoDeviceMode(): boolean {
        if (this.currentSyncIsTwoDeviceMode !== null) return this.currentSyncIsTwoDeviceMode;
        return this.settings.enableTwoDeviceOptimizations && this.connections.size === 1;
    }

    get twoDevicePeerId(): string | null {
        return this.isTwoDeviceMode() ? Array.from(this.connections.keys())[0] : null;
    }

    getMyRole(peerId: string): DeviceRole {
        return this.settings.deviceId < peerId ? 'primary' : 'secondary';
    }

    // --- Version Vectors ---
    incrementVersion(path: string) {
        if (!this.twoDeviceState.fileVersions[path]) this.twoDeviceState.fileVersions[path] = {};
        this.twoDeviceState.fileVersions[path][this.settings.deviceId] = (this.twoDeviceState.fileVersions[path][this.settings.deviceId] || 0) + 1;
        this.debouncedSaveState();
    }

    mergeVersions(local: VersionVector, remote: VersionVector): VersionVector {
        const merged: VersionVector = {};
        const keys = new Set([...Object.keys(local), ...Object.keys(remote)]);
        for (const k of keys) {
            merged[k] = Math.max(local[k] || 0, remote[k] || 0);
        }
        return merged;
    }

    isNewerThan(v1: VersionVector, v2: VersionVector): boolean {
        let hasGreater = false;
        const keys = new Set([...Object.keys(v1), ...Object.keys(v2)]);
        for (const k of keys) {
            const val1 = v1[k] || 0;
            const val2 = v2[k] || 0;
            if (val1 < val2) return false;
            if (val1 > val2) hasGreater = true;
        }
        return hasGreater;
    }

    // --- Merkle Tree Vault Diffing ---
    async buildMerkleTree(): Promise<MerkleNode> {
        const tree: MerkleNode = { hash: '', children: {} };
        const allFiles = this.app.vault.getAllLoadedFiles();
        
        for (const file of allFiles) {
            if (file instanceof TFile && this.isPathSyncable(file.path)) {
                let hash = this.syncedHashes.get(file.path)?.hash;
                if (!hash) {
                    if (file.stat.size > 5 * 1024 * 1024) {
                        hash = `massive-${file.stat.size}-${file.stat.mtime}`;
                    } else {
                        const content = this.isBinary(file.extension) ? await this.app.vault.readBinary(file) : await this.app.vault.cachedRead(file);
                        hash = await this.getHash(content);
                    }
                    this.updateHashCache(file.path, hash);
                }
                
                const parts = file.path.split('/');
                let current = tree;
                for (let i = 0; i < parts.length; i++) {
                    const part = parts[i];
                    if (!current.children) current.children = {};
                    if (!current.children[part]) current.children[part] = { hash: '' };
                    current = current.children[part];
                    if (i === parts.length - 1) {
                        current.hash = hash;
                    }
                }
            }
        }

        const computeHashes = async (root: MerkleNode): Promise<string> => {
            // Iterative post-order traversal — prevents stack overflow on deeply nested vaults
            const stack: Array<{ node: MerkleNode; phase: 'push' | 'process' }> = [{ node: root, phase: 'push' }];
            while (stack.length > 0) {
                const entry = stack.pop()!;
                if (entry.phase === 'process') {
                    const node = entry.node;
                    if (node.children && Object.keys(node.children).length > 0) {
                        const childKeys = Object.keys(node.children).sort();
                        let combined = '';
                        for (const k of childKeys) combined += node.children[k].hash;
                        node.hash = await this.getHash(combined);
                    }
                } else {
                    stack.push({ node: entry.node, phase: 'process' });
                    if (entry.node.children) {
                        for (const child of Object.values(entry.node.children)) {
                            stack.push({ node: child, phase: 'push' });
                        }
                    }
                }
            }
            return root.hash;
        };

        await computeHashes(tree);
        this.twoDeviceState.merkleTreeRoot = tree;
        this.debouncedSaveState();
        return tree;
    }

    // --- State Management ---
    async loadState() {
        const bakPath = this.statePath + '.bak';
        const tryLoad = async (path: string): Promise<any | null> => {
            try {
                if (await this.app.vault.adapter.exists(path)) {
                    return JSON.parse(await this.app.vault.adapter.read(path));
                }
            } catch (e) {
                console.error(`Failed to parse state from ${path}:`, e);
            }
            return null;
        };

        let state = await tryLoad(this.statePath);
        if (!state) {
            console.warn('Primary state.json failed — attempting backup recovery...');
            state = await tryLoad(bakPath);
            if (state) this.showNotice('Recovered sync state from backup (state.json.bak).', 'warning');
        }
        if (!state) return;

        if (state.activeTransfers) {
            for (const t of state.activeTransfers) {
                this.activeTransfers.set(t.id, { ...t, status: 'paused' });
            }
            this.updateStatus();
        }
        if (state.failedSyncs) this.failedSyncs = state.failedSyncs;
        if (state.tombstones) this.tombstones = state.tombstones;
        if (state.twoDeviceState) {
            this.twoDeviceState = state.twoDeviceState;
            if (!this.twoDeviceState.fileVersions) this.twoDeviceState.fileVersions = {};
        }
        if (state.syncedHashes) {
            for (const [p, d] of Object.entries(state.syncedHashes)) {
                this.syncedHashes.set(p, d as any);
            }
        }
        await this.loadQueueState();
    }

    async saveState() {
        const state = {
            activeTransfers: Array.from(this.activeTransfers.values()),
            failedSyncs: this.failedSyncs,
            twoDeviceState: this.twoDeviceState,
            tombstones: this.tombstones,
            syncedHashes: Object.fromEntries(this.syncedHashes)
        };
        const json = JSON.stringify(state, null, 2);
        const tmpPath = this.statePath + '.tmp';
        const bakPath = this.statePath + '.bak';
        try {
            // Stage into .tmp first so a mid-write crash doesn't corrupt the main file
            await this.app.vault.adapter.write(tmpPath, json);
            // Snapshot the last-known-good state before overwriting
            if (await this.app.vault.adapter.exists(this.statePath)) {
                const prev = await this.app.vault.adapter.read(this.statePath);
                await this.app.vault.adapter.write(bakPath, prev);
            }
            await this.app.vault.adapter.write(this.statePath, json);
            if (await this.app.vault.adapter.exists(tmpPath)) {
                await this.app.vault.adapter.remove(tmpPath);
            }
        } catch (e) {
            console.error('Failed to save state:', e);
        }
        await this.saveQueueState();
    }
    
    updateHashCache(path: string, hash: string) {
        if (this.syncedHashes.has(path)) {
            this.syncedHashes.delete(path);
        }
        this.syncedHashes.set(path, { hash, timestamp: Date.now() });
        if (this.syncedHashes.size > MAX_HASH_CACHE_SIZE) {
            const oldestPath = this.syncedHashes.keys().next().value;
            if (oldestPath) this.syncedHashes.delete(oldestPath);
        }
        this.debouncedSaveState();
    }

    pruneTombstones() {
        const retentionMs = (this.settings.tombstoneRetentionDays || 30) * 24 * 60 * 60 * 1000;
        const now = Date.now();
        let pruned = false;
        for (const [path, timestamp] of Object.entries(this.tombstones)) {
            if (now - timestamp > retentionMs) {
                delete this.tombstones[path];
                if (this.twoDeviceState.fileVersions) delete this.twoDeviceState.fileVersions[path];
                pruned = true;
            }
        }
        if (pruned) this.debouncedSaveState();
    }

    async loadSettings() { 
        this.settings = Object.assign({}, DEFAULT_SETTINGS, await this.loadData()); 
        // Invalidate folder filter caches whenever settings are (re-)loaded
        this._cachedExcludedFolders = null;
        this._cachedIncludedFolders = null;
        if (!this.settings.deviceId) {
            this.settings.deviceId = `device-${Array.from(window.crypto.getRandomValues(new Uint8Array(4))).map(b => b.toString(16).padStart(2, '0')).join('')}`;
            await this.saveData(this.settings);
        }
        if (!this.settings.peerKeys) this.settings.peerKeys = {};
        this.applyHideNativeSync(); 
        if (this.settings.knownPeers) {
            this.settings.knownPeers.forEach(p => this.clusterPeers.set(p.deviceId, p));
        }
    }
    async saveSettings() { 
        await this.saveData(this.settings);
        // Invalidate folder filter caches so isPathSyncable picks up the new values immediately
        this._cachedExcludedFolders = null;
        this._cachedIncludedFolders = null;
    }
    async saveKnownPeers() { this.settings.knownPeers = Array.from(this.clusterPeers.values()); await this.saveSettings(); }

    public updateDebounceDelay() {
        this.debouncedHandleFileChange = debounce(this.handleFileChange.bind(this), this.settings.debounceDelay);
    }

    applyHideNativeSync() {
        if (this.settings.hideNativeSyncStatus) {
            document.body.classList.add('od-hide-native-sync');
        } else {
            document.body.classList.remove('od-hide-native-sync');
        }
    }

    public log(...args: any[]) { if (this.settings.verboseLogging) { console.log("Obsidian Decentralized:", ...args); } }

    private async runLocked(path: string, callback: () => Promise<void>) {
        const existingLock = this.fileLocks.get(path) || Promise.resolve();
        const newLock = existingLock.catch(() => {}).then(async () => {
            await callback();
        }).catch(err => {
            this.log(`Lock error for ${path}:`, err);
            throw err; // Fix: Rethrow to prevent swallowing errors
        }).finally(() => {
            // Fix: Delete path from fileLocks map if it is the last lock in the chain to prevent memory leaks
            if (this.fileLocks.get(path) === newLock) {
                this.fileLocks.delete(path);
            }
        });
        this.fileLocks.set(path, newLock);
        return newLock;
    }

    public showNotice(message: string, level: 'info' | 'verbose' | 'error' | 'important' | 'warning' = 'info', timeout?: number) {
        if (level === 'error') {
            new Notice(`[Error] ${message}`, timeout || 10000);
            return;
        }
        if (level === 'warning') {
            new Notice(`[Warning] ${message}`, timeout || 8000);
            return;
        }
        if (level === 'important') {
            new Notice(message, timeout);
            return;
        }
        if (!this.settings.showToasts) {
            return;
        }
        if (level === 'verbose' && !this.settings.verboseLogging) {
            return;
        }
        new Notice(message, timeout);
    }

    public getConflictStrategy() { 
        if (this.isTwoDeviceMode() && this.settings.enableTwoDeviceOptimizations) return 'role-based';
        return this.settings.syncMode === 'auto' ? 'create-conflict-file' : this.settings.conflictResolutionStrategy; 
    }
    public shouldSyncAllFileTypes() { return this.settings.syncMode === 'auto' ? true : this.settings.syncAllFileTypes; }
    public shouldSyncObsidianConfig() { return this.settings.syncMode === 'auto' ? true : this.settings.syncObsidianConfig; }
    // Respect the explicit connectionMode even in 'auto' sync mode: hard-forcing
    // 'peerjs' here made the "Switch to Offline Mode" UI a no-op for default-profile
    // users — the UI showed Direct-IP while the runtime kept using PeerJS.
    public getConnectionMode() { return this.settings.connectionMode || 'peerjs'; }
    private hasPeers(): boolean {
        if (this.getConnectionMode() === 'direct-ip') {
            return !!this.directIpClient || !!this.directIpServer;
        }
        return this.connections.size > 0;
    }

    private generateTransferId(path: string): string { return `${path}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`; }
    
    // --- File System Events ---
    private handleEvent(file: TAbstractFile) { 
        if (this.shouldIgnoreEvent(file.path)) return; 
        if (!this.isPathSyncable(file.path)) return; 
        if (!this.hasPeers()) return; 
        
        if (this.isTwoDeviceMode() && this.remoteLocks.has(file.path)) {
            const lock = this.remoteLocks.get(file.path)!;
            if (Date.now() < lock.expiresAt) {
                this.showNotice(`File ${file.name} is locked by peer. Sync delayed.`, 'warning');
                // Actually delay: re-run this event once the peer's lock expires
                this.timeoutManager.setTimeout(() => this.handleEvent(file), (lock.expiresAt - Date.now()) + 250);
                return;
            } else {
                this.remoteLocks.delete(file.path);
            }
        }

        if (!this.app.vault.getAbstractFileByPath(file.path)) { 
            this.handleFileDelete(file); 
            return; 
        } 
        this.debouncedHandleFileChange(file); 
    }
    
    private async handleFileChange(file: TAbstractFile) { 
        await this.runLocked(file.path, async () => { 
            this.log(`Processing debounced change for: ${file.path}`); 
            
            if (this.isTwoDeviceMode() && !this.heldLocks.has(file.path) && file instanceof TFile && !this.isBinary(file.extension)) {
                await this.requestLock(file.path);
            }

            if (file instanceof TFile) { 
                if (this.isTwoDeviceMode()) this.incrementVersion(file.path);
                this.syncedHashes.delete(file.path);
                await this.sendFileUpdate(file); 
            } else if (file instanceof TFolder) { 
                this.addToQueueTask(null, { taskType: 'send-folder-create', path: file.path });
            } 
        }); 
    }
    
    private async handleFileDelete(file: TAbstractFile) { 
        await this.runLocked(file.path, async () => { 
            if (this.shouldIgnoreEvent(file.path)) return; 
            if (!this.isPathSyncable(file.path)) return; 
            this.log(`Processing delete: ${file.path}`); 
            this.syncedHashes.delete(file.path);
            if (file instanceof TFile) { 
                if (this.isTwoDeviceMode()) this.incrementVersion(file.path); 
                this.tombstones[file.path] = Date.now(); 
                this.debouncedSaveState(); 
                this.addToQueueTask(null, { taskType: 'send-delete', path: file.path }); 
            } else if (file instanceof TFolder) { 
                this.broadcastData({ type: 'folder-delete', path: file.path, transferId: this.generateTransferId(file.path) }); 
            } 
        }); 
    }
    
    private async handleRenameEvent(file: TAbstractFile, oldPath: string) { 
        if (oldPath === file.path) return;
        // Acquire locks in sorted order to prevent deadlock between concurrent renames (e.g. A→B and B→A)
        const [firstLock, secondLock] = [oldPath, file.path].sort();
        await this.runLocked(firstLock, async () => { 
            await this.runLocked(secondLock, async () => { 
                if (this.shouldIgnoreEvent(oldPath) || this.shouldIgnoreEvent(file.path)) return; 
                if (!this.isPathSyncable(file.path) && !this.isPathSyncable(oldPath)) return; 
                if (!this.hasPeers()) return; 
                this.log(`Processing rename: ${oldPath} -> ${file.path}`); 
                this.ignoreNextEventForPath(file.path); 
                this.ignoreNextEventForPath(oldPath); 
                
                const cached = this.syncedHashes.get(oldPath);
                if (cached) {
                    this.syncedHashes.set(file.path, cached);
                    this.syncedHashes.delete(oldPath);
                    this.debouncedSaveState();
                }
                const vector = this.twoDeviceState.fileVersions[oldPath];
                if (vector) {
                    this.twoDeviceState.fileVersions[file.path] = vector;
                    delete this.twoDeviceState.fileVersions[oldPath];
                    this.debouncedSaveState();
                }
                
                if (file instanceof TFile) { 
                    if (this.isTwoDeviceMode()) { this.incrementVersion(oldPath); this.incrementVersion(file.path); } 
                    this.addToQueueTask(null, { taskType: 'send-rename', oldPath, newPath: file.path }); 
                } else if (file instanceof TFolder) { 
                    this.broadcastData({ type: 'folder-rename', oldPath, newPath: file.path, transferId: this.generateTransferId(file.path) }); 
                } 
            }); 
        }); 
    }

    // --- Real-time Editor Sync ---
    private handleEditorChange(editor: any, info: any) {
        if (!this.isTwoDeviceMode() || !this.settings.enableRealtimeSync) return;
        
        const view = this.app.workspace.getActiveViewOfType(MarkdownView);
        if (!view || !view.file) return;
        const path = view.file.path;

        if (this.isApplyingRemoteEdit || this.shouldIgnoreEvent(path)) return;

        if (!this.heldLocks.has(path)) {
            // Await lock before sending edits to avoid racing with peer's active editing
            this.requestLock(path).then(granted => {
                if (granted) {
                    this.sendData(this.twoDevicePeerId!, { type: 'editor-active', path });
                }
            });
        }

        this.debouncedEditorChange(editor, view.file);
    }

    private async handleEditorChangeDebounced(editor: any, file: TFile) {
        if (!this.isTwoDeviceMode() || !this.settings.enableRealtimeSync) return;
        const path = file.path;
        
        const currentText = editor.getValue();
        const cached = this.lastSentContent.get(path);
        
        if (cached) {
            const dmp = new DiffMatchPatch();
            const patches = dmp.patch_make(cached.content, currentText);
            if (patches.length > 0) {
                const patchText = dmp.patch_toText(patches);
                const payload: EditorDeltaPayload = { type: 'editor-delta', path, patches: patchText };
                this.sendData(this.twoDevicePeerId!, payload);
            }
        }
        this.lastSentContent.set(path, { content: currentText, timestamp: Date.now() });
    }

    // --- PSK Encryption ---
    async generatePSK(): Promise<string> {
        const key = await window.crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
        const exported = await window.crypto.subtle.exportKey('raw', key);
        return arrayBufferToBase64(exported);
    }

    /**
     * Imported AES-GCM keys, cached per peer. Previously every single message —
     * including every 512 KB chunk — re-derived the key from base64 and called
     * importKey, which dominated the cost of encrypted transfers.
     */
    private cryptoKeys: Map<string, CryptoKey> = new Map();

    private async getCryptoKey(peerId: string): Promise<CryptoKey | null> {
        const cached = this.cryptoKeys.get(peerId);
        if (cached) return cached;
        const psk = this.settings.peerKeys[peerId];
        if (!psk) return null;
        const key = await window.crypto.subtle.importKey(
            'raw', base64ToArrayBuffer(psk), { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']
        );
        this.cryptoKeys.set(peerId, key);
        return key;
    }

    /** Drop a cached key so a rotated or removed PSK is never reused. */
    public invalidateCryptoKey(peerId?: string) {
        if (peerId) this.cryptoKeys.delete(peerId);
        else this.cryptoKeys.clear();
    }

    /**
     * Encrypt a message into the V3 wire envelope: { type:'encrypted-frame', data }
     * where data is [12B IV][AES-GCM ciphertext] and the plaintext is a single
     * packFrame buffer of [headerLen][header JSON][raw binary body].
     *
     * Binary bodies stay binary end to end. The 2.x envelope base64'd them into
     * JSON and then base64'd the ciphertext again, inflating the wire by ~33% and
     * copying the buffer three extra times per message.
     */
    async encryptPayload(data: any, peerId: string): Promise<any> {
        const key = await this.getCryptoKey(peerId);
        if (!key) throw new Error(`No encryption key for peer ${peerId}`);
        try {
            const { header, body } = splitBinaryPayload(data);
            const plaintext = packFrame(header, body);
            const iv = window.crypto.getRandomValues(new Uint8Array(12));
            const ciphertext = await window.crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, plaintext);

            const framed = new Uint8Array(12 + ciphertext.byteLength);
            framed.set(iv, 0);
            framed.set(new Uint8Array(ciphertext), 12);
            return { type: 'encrypted-frame', data: framed.buffer };
        } catch (e) {
            // Never fall back to plaintext — throw so the caller halts the send.
            this.log('Encryption failed', e);
            throw new Error(`Encryption failed: ${e instanceof Error ? e.message : String(e)}`);
        }
    }

    async decryptPayload(encryptedPayload: any, peerId: string): Promise<any> {
        const key = await this.getCryptoKey(peerId);
        if (!key) throw new Error(`No decryption key for peer ${peerId}`);
        const raw = encryptedPayload.data;
        const buf: ArrayBuffer = raw instanceof ArrayBuffer
            ? raw
            : (raw as Uint8Array).buffer.slice((raw as Uint8Array).byteOffset, (raw as Uint8Array).byteOffset + (raw as Uint8Array).byteLength);
        if (buf.byteLength < 13) throw new Error('Decryption failed: frame too short');
        try {
            const iv = new Uint8Array(buf, 0, 12);
            const decrypted = await window.crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, buf.slice(12));
            const { header, body } = unpackFrame(decrypted);
            return joinBinaryPayload(header, body);
        } catch (e) {
            this.log('Decryption failed', e);
            throw new Error('Decryption failed');
        }
    }

    // --- Communication Layer ---
    broadcastData(data: SyncData) { this.addToQueue(null, data); }
    sendData(peerId: string, data: SyncData) { this.addToQueue(peerId, data); }
    
    private computePriority(data: SyncData): number {
        if (data.type === 'editor-delta' || data.type === 'editor-active' || data.type.startsWith('lock-')) return 1000000; 

        const controlMessages = ['request-full-sync', 'sync-plan', 'request-batch', 'batch-complete', 'full-sync-complete', 'sync-control-json'];
        if (controlMessages.includes(data.type)) return 500000;

        if (data.type === 'folder-create' || data.type === 'folder-delete' || data.type === 'folder-rename') return 100000;
        if (data.type === 'file-delete' || data.type === 'file-rename' || data.type === 'file-delta') return 50000;
        if (data.type === 'file-update') {
            const size = data.content instanceof ArrayBuffer ? data.content.byteLength : (typeof data.content === 'string' ? data.content.length : 0);
            return Math.max(0, 10000 - size);
        }
        return -1;
    }
    
    private computePriorityTask(task: SyncTask): number {
        if (task.taskType === 'send-folder-create' || task.taskType === 'send-delete' || task.taskType === 'send-rename') return 100000;
        return 50000;
    }

    private addToQueue(peerId: string | null, data: SyncData) { 
        const priority = this.computePriority(data);
        this.queueManager.addToQueue({ peerId, data, retries: 0, priority });
        this.debouncedSaveState();
    }
    
    private addToQueueTask(peerId: string | null, task: SyncTask) { 
        const priority = this.computePriorityTask(task);
        this.queueManager.addToQueue({ peerId, task, retries: 0, priority });
        this.debouncedSaveState();
    }

    public getQueuePressure(): number {
        return this.queueManager.getQueuePressure();
    }

    public transitionToPhase(newPhase: SyncPhase) {
        this.log(`Sync phase transition: ${this.syncState.currentPhase} -> ${newPhase}`);
        this.syncState.currentPhase = newPhase;
        this.syncState.phaseStartTime = Date.now();
        if (this.syncState.phaseTimeoutHandle) { clearTimeout(this.syncState.phaseTimeoutHandle); this.syncState.phaseTimeoutHandle = null; }
        
        let timeoutMs = 0;
        if (newPhase === SyncPhase.REQUESTING) timeoutMs = REQUESTING_TIMEOUT;
        else if (newPhase === SyncPhase.PLANNING) timeoutMs = PLANNING_TIMEOUT;
        else if (newPhase === SyncPhase.TRANSFERRING) timeoutMs = BATCH_TIMEOUT;
        else if (newPhase === SyncPhase.COMPLETING) timeoutMs = COMPLETING_TIMEOUT;

        if (timeoutMs > 0) {
            this.syncState.phaseTimeoutHandle = window.setTimeout(() => {
                this.abortSync(new SyncError(SyncErrorCategory.TIMEOUT_ERROR, `Sync timed out during ${newPhase}.`, false, "The peer may be busy or have a slow connection. Try again later."));
            }, timeoutMs);
        }
        this.updateStatus();
    }

    public isConnectionHealthy(peerId: string): boolean {
        if (this.getConnectionMode() === 'direct-ip') {
            if (this.directIpClient) {
                return this.directIpClient.isOpen && peerId === 'direct-ip-host';
            }
            if (this.directIpServer) {
                return this.directIpServer.hasClient(peerId);
            }
            return false;
        }
        const conn = this.connections.get(peerId);
        if (!conn || !conn.open) return false;
        const lastSuccess = this.lastSuccessfulMessageTime.get(peerId);
        if (lastSuccess && (Date.now() - lastSuccess > 35000)) return false;
        return true;
    }

    public async sendSyncMessage(peerId: string, data: any, retryCount = 0, existingMessageId?: string): Promise<void> {
        if (!this.isConnectionHealthy(peerId)) {
            throw new SyncError(SyncErrorCategory.CONNECTION_ERROR, `Connection to ${peerId} is unhealthy.`, true, "Check network connection.");
        }
        // Reuse messageId on retries so the peer's ACK for any attempt resolves the original promise
        const messageId = existingMessageId || this.generateTransferId(data.type);
        // Wrap sync control messages as a JSON string inside a thin envelope.
        // This prevents PeerJS's msgpack serializer from recursively packing
        // large nested payloads (e.g. manifests with 20k+ items), which causes
        // a "Maximum call stack size exceeded" RangeError.
        const envelope = { type: 'sync-control-json', jsonPayload: JSON.stringify({ ...data, messageId }) };
        
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(async () => {
                this.pendingSyncAcks.delete(messageId);
                if (retryCount < 3) {
                    this.log(`Timeout sending ${data.type}, retrying (${retryCount + 1}/3)...`);
                    try {
                        await this.sendSyncMessage(peerId, data, retryCount + 1, messageId);
                        resolve();
                    } catch (e) { reject(e); }
                } else {
                    reject(new SyncError(SyncErrorCategory.TIMEOUT_ERROR, `Failed to deliver ${data.type} after 3 retries.`, false, "Check peer connection."));
                }
            }, 30000);
            
            this.pendingSyncAcks.set(messageId, { resolve: () => { clearTimeout(timeout); resolve(); }, reject: (e) => { clearTimeout(timeout); reject(e); } });
            this.sendData(peerId, envelope as any);
        });
    }

    public abortSync(error?: SyncError) {
        if (!this.syncState.isSyncing) return;
        this.transitionToPhase(SyncPhase.ABORTING);
        this.syncState.isSyncing = false;
        this.currentSyncIsTwoDeviceMode = null;
        this.syncDrainCallback = null;
        this.queueManager.clear();
        this.debouncedSaveState();
        this.activeTransfers.clear();
        this.syncState.pendingPulls.clear();
        this.syncState.allowedPulls.clear();
        this.syncState.activeBatches.clear();
        this.localSyncComplete.clear();
        this.peerSyncComplete.clear();
        this.pullRetries.clear();
        this.syncState.peerId = null;
        this.peerFileSizes = {};
        if (this.syncIdleTimeout) { clearTimeout(this.syncIdleTimeout); this.syncIdleTimeout = null; }
        if (this.syncKeepAliveInterval) { clearInterval(this.syncKeepAliveInterval); this.syncKeepAliveInterval = null; }
        if (this.syncState.phaseTimeoutHandle) { clearTimeout(this.syncState.phaseTimeoutHandle); this.syncState.phaseTimeoutHandle = null; }
        
        const errorMessage = error ? error.message : "Sync aborted manually.";
        this.pendingAcks.forEach(ack => ack.reject(new Error(errorMessage)));
        this.pendingAcks.clear();
        this.pendingSyncAcks.forEach(ack => ack.reject(new Error(errorMessage)));
        this.pendingSyncAcks.clear();
        
        if (error) {
            this.showNotice(`Sync failed: ${error.message}\nAction: ${error.suggestedAction}`, 'error', 10000);
            this.log(`Sync aborted [${error.category}]: ${error.message}`);
        } else {
            this.showNotice(`Sync aborted.`, 'warning', 5000);
            this.log(`Sync aborted manually.`);
        }
        
        this.syncState.currentPhase = SyncPhase.IDLE;
        this.updateStatus();
    }

    public getConcurrencyLimit() { 
        if (this.settings.maximumConcurrentTransfers) return this.settings.maximumConcurrentTransfers;
        if (this.getConnectionMode() === 'direct-ip') return Math.max(this.currentConcurrency, 50);
        return this.currentConcurrency; 
    }
    
    public getChunkSize() { 
        if (this.settings.chunkSize) return this.settings.chunkSize;
        if (this.getConnectionMode() === 'direct-ip') return 2 * 1024 * 1024; // 2MB for direct-ip
        return this.currentChunkSize; 
    }

    private recordTransferSample(bytes: number, durationMs: number) {
        if (durationMs <= 0 || bytes <= 0) return;
        this.recentTransferSamples.push({ bytes, durationMs });
        if (this.recentTransferSamples.length > MAX_BANDWIDTH_SAMPLES) {
            this.recentTransferSamples.shift();
        }
        
        let totalBytes = 0;
        let totalMs = 0;
        for (const sample of this.recentTransferSamples) {
            totalBytes += sample.bytes;
            totalMs += sample.durationMs;
        }
        
        this.currentBandwidthEstimate = (totalBytes / totalMs) * 1000;
        this.adaptChunkSize();
    }

    private adaptChunkSize() {
        if (this.currentBandwidthEstimate > 0) {
            const targetSize = Math.floor(this.currentBandwidthEstimate * (TARGET_CHUNK_TIME_MS / 1000));
            this.targetChunkSize = Math.max(MIN_CHUNK_SIZE, Math.min(MAX_CHUNK_SIZE, targetSize));
        }
    }

    private reportTransferResult(success: boolean) {
        if (success) {
            this.successfulTransfersSinceLastIncrease++;
            if (this.successfulTransfersSinceLastIncrease >= this.currentConcurrency) {
                const maxConcurrency = this.getConnectionMode() === 'direct-ip' ? 200 : 32;
                if (this.currentConcurrency < maxConcurrency) {
                    this.currentConcurrency = Math.min(maxConcurrency, this.currentConcurrency + Math.max(1, Math.floor(this.currentConcurrency * 0.1)));
                    this.log(`Network stable. Increasing concurrency to ${this.currentConcurrency}`);
                }
                if (this.currentChunkSize < this.targetChunkSize && this.getConnectionMode() !== 'direct-ip') {
                    this.currentChunkSize = Math.min(this.targetChunkSize, Math.floor(this.currentChunkSize * 1.5));
                    this.log(`Network stable. Increasing chunk size to ${this.currentChunkSize}`);
                }
                this.successfulTransfersSinceLastIncrease = 0;
            }
        } else {
            const newLimit = Math.max(1, Math.floor(this.currentConcurrency * 0.7));
            if (newLimit < this.currentConcurrency) {
                this.currentConcurrency = newLimit;
                this.log(`Network issues detected. Decreasing concurrency to ${this.currentConcurrency}`);
            }
            if (this.getConnectionMode() !== 'direct-ip') {
                const newChunkSize = Math.max(MIN_CHUNK_SIZE, Math.floor(this.currentChunkSize * 0.75));
                if (newChunkSize < this.currentChunkSize) {
                    this.currentChunkSize = newChunkSize;
                    this.log(`Network issues. Decreasing chunk size to ${this.currentChunkSize}`);
                }
            }
            this.successfulTransfersSinceLastIncrease = 0;
        }
    }

    resetIdleTimeout() {
        this.timeoutManager.clearTimeout(this.syncIdleTimeout);
        if (this.syncState.isSyncing) {
            this.syncIdleTimeout = this.timeoutManager.setTimeout(() => {
                this.abortSync(new SyncError(SyncErrorCategory.TIMEOUT_ERROR, "Sync idle timeout reached. Connection may have dropped.", false, "Check network connection."));
            }, this.settings.idleTimeoutMs || 30000);
        }
    }

    private processQueue() {
        this.queueManager.setConcurrencyLimit(this.getConcurrencyLimit());
        if (this.syncDrainCallback) {
            this.queueManager.setSyncDrainCallback(this.syncDrainCallback);
            this.syncDrainCallback = null;
        }
        this.queueManager.resume();
    }

    private async processQueueItem(item: { peerId: string | null, task?: SyncTask, data?: any, retries: number, priority: number }) {
        let transferId: string | undefined;
        let isPaused = false;
        let success = false;
        const startTime = Date.now();

        try {
            let { peerId } = item;
            
            if (item.task) {
                const task = item.task;
                if (task.taskType === 'send-file') {
                    const file = this.app.vault.getAbstractFileByPath(task.path);
                    if (file instanceof TFile) {
                        if (this.syncState.isSyncing) {
                            this.syncState.currentFile = file.path;
                            this.syncState.currentFileSize = file.stat.size;
                        }
                        // NOTE: lastSentContent eviction is handled by the 60-s cleanupPendingChunks interval
                        let content: string | ArrayBuffer = this.isBinary(file.extension) ? await this.app.vault.readBinary(file) : await this.app.vault.read(file);
                        let encoding: 'utf8' | 'binary' | 'base64' = this.isBinary(file.extension) ? 'binary' : 'utf8';
                        let hash = '';
                        try { hash = await this.getHash(content); } catch(e) {}
                        
                        if (!task.forceFull && this.syncedHashes.get(file.path)?.hash === hash) {
                            this.log(`Ignoring echo event for ${file.path}`);
                            success = true;
                            return;
                        }
                        if (hash) this.updateHashCache(file.path, hash);
                        
                        let isCompressedText = false;
                        let vv = this.isTwoDeviceMode() ? this.twoDeviceState.fileVersions[file.path] : undefined;
                        
                        if (!this.isBinary(file.extension)) {
                            if (this.settings.enableDeltaSync && !task.forceFull) {
                                const cached = this.lastSentContent.get(file.path);
                                const newText = content as string;
                                if (cached) {
                                    const dmp = new DiffMatchPatch();
                                    const patches = dmp.patch_make(cached.content, newText);
                                    const patchText = dmp.patch_toText(patches);
                                    if (patchText.length < newText.length * (this.settings.deltaSyncThreshold / 100)) {
                                        const baseHash = await this.getHash(cached.content);
                                        item.data = {
                                            type: 'file-delta',
                                            path: file.path,
                                            mtime: file.stat.mtime,
                                            patches: patchText,
                                            baseHash,
                                            versionVector: vv,
                                            transferId: this.generateTransferId(file.path)
                                        };
                                        this.lastSentContent.set(file.path, { content: newText, timestamp: Date.now() });
                                    }
                                }
                                if (!item.data) this.lastSentContent.set(file.path, { content: newText, timestamp: Date.now() });
                            }
                            
                            if (!item.data && this.settings.enableCompression) {
                                content = compressText(content as string);
                                encoding = 'binary';
                                isCompressedText = true;
                            }
                        }
                        
                        if (!item.data) {
                            if (typeof content === 'string') {
                                const encoded = ObsidianDecentralizedPlugin.textEncoder.encode(content);
                                if (encoded.byteLength > this.getChunkSize()) {
                                    content = encoded.buffer;
                                    encoding = 'binary';
                                }
                            }
                            item.data = { type: 'file-update', path: file.path, content, mtime: file.stat.mtime, encoding, transferId: this.generateTransferId(file.path), fileHash: hash, compressed: isCompressedText, versionVector: vv };
                        }
                    } else {
                        success = true;
                        return;
                    }
                } else if (task.taskType === 'send-delete') {
                    item.data = { type: 'file-delete', path: task.path, transferId: this.generateTransferId(task.path) };
                } else if (task.taskType === 'send-folder-create') {
                    item.data = { type: 'folder-create', path: task.path, transferId: this.generateTransferId(task.path) };
                } else if (task.taskType === 'send-rename') {
                    let vv = this.isTwoDeviceMode() ? this.twoDeviceState.fileVersions[task.newPath] : undefined;
                    item.data = { type: 'file-rename', oldPath: task.oldPath, newPath: task.newPath, transferId: this.generateTransferId(task.newPath), versionVector: vv };
                } else if (task.taskType === 'send-file-batch') {
                    const packedFiles: PackedFile[] = [];
                    for (const path of task.paths) {
                        const file = this.app.vault.getAbstractFileByPath(path);
                        if (file instanceof TFile) {
                            let content: string | ArrayBuffer = this.isBinary(file.extension) ? await this.app.vault.readBinary(file) : await this.app.vault.read(file);
                            let encoding: 'utf8' | 'binary' | 'base64' = this.isBinary(file.extension) ? 'binary' : 'utf8';
                            let isCompressedText = false;
                            
                            if (!this.isBinary(file.extension) && this.settings.enableCompression) {
                                content = compressText(content as string);
                                encoding = 'binary';
                                isCompressedText = true;
                            }
                            
                            if (typeof content === 'string') {
                                const encoded = ObsidianDecentralizedPlugin.textEncoder.encode(content);
                                content = encoded;
                                encoding = 'binary';
                            }
                            
                            packedFiles.push({
                                path: file.path,
                                mtime: file.stat.mtime,
                                isCompressed: isCompressedText,
                                encoding,
                                content: content as ArrayBuffer
                            });
                        }
                    }
                    if (packedFiles.length > 0) {
                        item.data = {
                            type: 'file-batch-binary',
                            batchId: task.batchId,
                            transferId: this.generateTransferId('batch-' + task.batchId),
                            data: packFilesToTLV(packedFiles)
                        };
                    } else {
                        success = true;
                        return;
                    }
                }
            }
            
            const data = item.data;
            if (!data) return;
            transferId = data.transferId;

            if (!peerId && (data.type === 'file-update' || data.type === 'file-delta')) {
                let connectedPeers: string[] = [];
                if (this.getConnectionMode() === 'direct-ip') {
                    if (this.directIpServer) connectedPeers = this.directIpServer.getClients();
                    else if (this.directIpClient && this.directIpClient.isOpen) connectedPeers = ['direct-ip-host'];
                } else {
                    connectedPeers = Array.from(this.connections.keys());
                }

                if (connectedPeers.length === 0) {
                    // No peers right now (e.g. queue restored from disk before connections
                    // came up). Don't discard silently — park in failedSyncs so
                    // retryFailedSyncs() re-sends once a peer reconnects.
                    if (data.type === 'file-update' || data.type === 'file-delta') {
                        const existing = this.failedSyncs.find(f => f.path === data.path && !f.peerId);
                        if (!existing) {
                            this.failedSyncs.push({
                                path: data.path,
                                peerId: null,
                                timestamp: Date.now(),
                                type: data.type,
                                reason: 'No peers connected',
                                retryCount: 0
                            });
                            this.debouncedSaveState();
                        }
                    }
                    success = true;
                    return;
                }
                
                peerId = connectedPeers[0];
                
                for (let i = 1; i < connectedPeers.length; i++) {
                    const newData = { ...data, transferId: this.generateTransferId(data.path) };
                    this.addToQueue(connectedPeers[i], newData);
                }
            }

            const isChunkedTransfer = data.type === 'file-update' && data.content instanceof ArrayBuffer && data.content.byteLength > this.getChunkSize();

            if (isChunkedTransfer) {
                const fileData = data as FileUpdatePayload;
                if (!transferId) throw new Error("Transfer ID missing for chunked transfer");
                
                if (peerId) {
                    const ackPromise = new Promise<void>((resolve, reject) => {
                        const timeout = setTimeout(() => reject(new Error(`Transfer ${transferId} timed out`)), 300000);
                        this.pendingAcks.set(transferId!, {
                            resolve: () => { clearTimeout(timeout); resolve(); },
                            reject: (e) => { clearTimeout(timeout); reject(e); },
                            peerId: peerId!
                        });
                    });

                    await this.sendFileInChunks(peerId, fileData.path, fileData.mtime, fileData.content as ArrayBuffer, transferId!, 0, fileData.compressed, fileData.versionVector);
                    await ackPromise;
                    this.log(`Chunked transfer ${transferId} for ${fileData.path} completed successfully.`);
                    
                    // Dereference to allow GC
                    fileData.content = null as any;
                    item.data = null as any;
                }
            } else {
                let finalPayload = data;
                if (this.settings.enableEncryption && peerId && this.settings.peerKeys[peerId]) {
                    finalPayload = await this.encryptPayload(data, peerId);
                }

                const isBatchItem = item.task && (item.task as any).batchId;
                const isSmallFile = (data.type === 'file-update' || data.type === 'file-delta');
                const isDirectIp = this.getConnectionMode() === 'direct-ip';
                const skipAck = (isBatchItem && isSmallFile && isDirectIp) || data.type === 'file-batch-binary';

                if (isSmallFile && peerId && !skipAck) {
                    const ackPromise = new Promise<void>((resolve, reject) => {
                        const timeout = setTimeout(() => reject(new Error(`Transfer ${transferId} timed out`)), 60000);
                        this.pendingAcks.set(transferId!, {
                            resolve: () => { clearTimeout(timeout); resolve(); },
                            reject: (e) => { clearTimeout(timeout); reject(e); },
                            peerId: peerId!
                        });
                    });

                    if (isDirectIp) {
                        if (this.directIpClient) this.directIpClient.send(finalPayload);
                        else if (this.directIpServer) this.directIpServer.sendTo(peerId, finalPayload);
                    } else {
                        const conn = this.connections.get(peerId);
                        if (conn?.open) {
                            const dc = (conn as any)?.dataChannel || (conn as any)?._dc;
                            if (dc && dc.bufferedAmount > 2 * 1024 * 1024) {
                                await new Promise<void>(resolve => {
                                    if (dc.bufferedAmount <= 1 * 1024 * 1024) return resolve();
                                    const oldHandler = dc.onbufferedamountlow;
                                    dc.bufferedAmountLowThreshold = 1 * 1024 * 1024;
                                    dc.onbufferedamountlow = () => {
                                        dc.onbufferedamountlow = oldHandler;
                                        resolve();
                                    };
                                });
                            }
                            conn.send(finalPayload);
                        }
                        else throw new Error("Connection closed");
                    }
                    await ackPromise;
                    
                    if (data.type === 'file-update') {
                        (data as FileUpdatePayload).content = null as any;
                        item.data = null as any;
                    }
                } else if (skipAck && peerId) {
                    // Batch transfer: fire-and-forget, batch-complete handles reliability
                    if (isDirectIp) {
                        if (this.directIpClient) this.directIpClient.send(finalPayload);
                        else if (this.directIpServer) this.directIpServer.sendTo(peerId, finalPayload);
                    } else {
                        const conn = this.connections.get(peerId);
                        if (conn?.open) {
                            const dc = (conn as any)?.dataChannel || (conn as any)?._dc;
                            if (dc && dc.bufferedAmount > 2 * 1024 * 1024) {
                                await new Promise<void>(resolve => {
                                    if (dc.bufferedAmount <= 1 * 1024 * 1024) return resolve();
                                    const oldHandler = dc.onbufferedamountlow;
                                    dc.bufferedAmountLowThreshold = 1 * 1024 * 1024;
                                    dc.onbufferedamountlow = () => {
                                        dc.onbufferedamountlow = oldHandler;
                                        resolve();
                                    };
                                });
                            }
                            conn.send(finalPayload);
                        } else throw new Error("Connection closed");
                    }
                    
                    if (data.type === 'file-update' || data.type === 'file-batch-binary') {
                        (data as any).content = null;
                        (data as any).data = null;
                        item.data = null as any;
                    }
                } else
                if (this.getConnectionMode() === 'direct-ip') {
                    if (this.directIpClient) this.directIpClient.send(finalPayload);
                    else if (this.directIpServer) {
                        // Route to the addressed peer only — broadcasting a targeted message
                        // (e.g. a handshake response or lock grant) misdelivers it to every client
                        if (peerId) this.directIpServer.sendTo(peerId, finalPayload);
                        else this.directIpServer.send(finalPayload);
                    }
                } else {
                    const peersToSend = peerId ? [peerId] : Array.from(this.connections.keys());
                    peersToSend.forEach(async pId => {
                        let pPayload = data;
                        if (this.settings.enableEncryption && this.settings.peerKeys[pId]) {
                            pPayload = await this.encryptPayload(data, pId);
                        }
                        const conn = this.connections.get(pId);
                        if (conn?.open) { conn.send(pPayload); }
                    });
                }
            }
            success = true;
            this.resetIdleTimeout();
            
            let bytesTransferred = 0;
            if (data.type === 'file-update') {
                bytesTransferred = data.content instanceof ArrayBuffer ? data.content.byteLength : (typeof data.content === 'string' ? data.content.length : 0);
            } else if (data.type === 'file-delta') {
                bytesTransferred = (data as FileDeltaPayload).patches.length;
            }
            if (bytesTransferred > 0) {
                this.recordTransferSample(bytesTransferred, Date.now() - startTime);
            }

        } catch (e) {
            // Fix: Clear and resolve pending timeouts in pendingAcks to prevent memory leaks and unhandled promise rejections
            if (transferId && this.pendingAcks.has(transferId)) {
                const ack = this.pendingAcks.get(transferId);
                ack?.resolve();
                this.pendingAcks.delete(transferId);
            }

            if (e.message === 'Paused') {
                this.log(`Transfer ${transferId} paused due to connection loss.`);
                isPaused = true;
                return;
            }
            if (e instanceof Error && e.message.includes('IntegrityError')) {
                this.log(`Integrity failure for transfer ${transferId}.`);
                if (item.data && item.data.type === 'file-delta') {
                    const file = this.app.vault.getAbstractFileByPath(item.data.path);
                    if (file instanceof TFile) {
                        this.sendFileUpdate(file, item.peerId || undefined, true);
                    }
                    return;
                } else {
                    this.log(`Re-queueing.`);
                }
            } else {
                console.error(`Error processing queue item ${transferId}:`, e);
            }
            
            // QueueManager handles the retry backoff safely now.
            // We just need to record permanently failed syncs.
            if (item && item.retries >= 3) {
                const taskPath = item.task ? (item.task.taskType === 'send-rename' ? item.task.newPath : (item.task.taskType === 'send-file-batch' ? item.task.paths[0] : item.task.path)) : undefined;
                const path = item.data?.path || taskPath || 'an item';
                this.showNotice(`File transfer failed permanently for ${path}.`, 'error', 8000);
                if (this.syncState.isSyncing) this.abortSync(new SyncError(SyncErrorCategory.CONNECTION_ERROR, "Transfer failed permanently.", false, "Check peer connection."));
                
                if (item.data && (item.data.type === 'file-update' || item.data.type === 'file-delete' || item.data.type === 'file-delta')) {
                    const existing = this.failedSyncs.find(f => f.path === item.data.path && f.peerId === item.peerId && f.type === item.data.type);
                    if (!existing) {
                        this.failedSyncs.push({
                            path: item.data.path,
                            peerId: item.peerId,
                            timestamp: Date.now(),
                            type: item.data.type as any,
                            reason: e instanceof Error ? e.message : String(e),
                            retryCount: 0
                        });
                    } else {
                        existing.timestamp = Date.now();
                        existing.reason = e instanceof Error ? e.message : String(e);
                    }
                    this.debouncedSaveState();
                }
                // Dereference
                if (item.data && item.data.type === 'file-update') item.data.content = null;
                item.data = null;
            }
        } finally {
            let isFinal = false;
            if (success) {
                isFinal = true;
            } else if (!isPaused && (!item || item.retries >= 3)) {
                isFinal = true;
            }

            if (transferId && !isPaused) {
                this.reportTransferResult(success);
                
                if (success) {
                    const taskPath = item.task ? (item.task.taskType === 'send-rename' ? item.task.newPath : (item.task.taskType === 'send-file-batch' ? item.task.paths[0] : item.task.path)) : undefined;
                    const path = item.data?.path || taskPath;
                    if (path) {
                        for (let i = this.failedSyncs.length - 1; i >= 0; i--) {
                            const f = this.failedSyncs[i];
                            if (f.path === path && f.peerId === item.peerId) {
                                if (item.data && (item.data.type === 'file-update' || item.data.type === 'file-delta') && 
                                    (f.type === 'file-update' || f.type === 'file-delta')) {
                                    this.failedSyncs.splice(i, 1);
                                } else if (item.data && item.data.type === f.type) {
                                    this.failedSyncs.splice(i, 1);
                                }
                            }
                        }
                    }
                }

                if (this.pendingAcks.has(transferId)) {
                    this.pendingAcks.get(transferId)!.resolve();
                    this.pendingAcks.delete(transferId);
                }
                this.activeTransfers.delete(transferId);
                this.debouncedSaveState();
            }

            if (isFinal && item.task && (item.task as any).batchId) {
                const t: any = item.task;
                // Report EVERY path the task covered, not just the first one
                const batchPaths: string[] = t.taskType === 'send-file-batch' ? t.paths : (t.path ? [t.path] : (t.newPath ? [t.newPath] : []));
                this.recordBatchTaskCompletion(t.batchId, batchPaths, success);
            }

            this.updateStatus();
        }
    }

    rejectPendingAck(transferId: string, reason: string) {
        if (this.pendingAcks.has(transferId)) {
            this.pendingAcks.get(transferId)!.reject(new Error(reason));
            this.pendingAcks.delete(transferId);
        }
    }

    async retryFailedSyncs() {
        if (this.failedSyncs.length === 0) return;
        if (!this.hasPeers()) return;

        const now = Date.now();
        let changed = false;

        for (let i = this.failedSyncs.length - 1; i >= 0; i--) {
            const fail = this.failedSyncs[i];
            const backoffMs = 30000 * Math.pow(2, fail.retryCount || 0);

            if (now - fail.timestamp > backoffMs) {
                if ((fail.retryCount || 0) >= 5) {
                    this.failedSyncs.splice(i, 1);
                    changed = true;
                    continue;
                }

                fail.retryCount = (fail.retryCount || 0) + 1;
                fail.timestamp = now;
                changed = true;

                this.log(`Retrying failed sync: ${fail.path} (Attempt ${fail.retryCount})`);
                
                if (fail.type === 'file-update' || fail.type === 'file-delta') {
                    const file = this.app.vault.getAbstractFileByPath(fail.path);
                    if (file instanceof TFile) {
                        this.sendFileUpdate(file, fail.peerId || undefined, true);
                    } else {
                        this.failedSyncs.splice(i, 1);
                    }
                } else if (fail.type === 'file-delete') {
                     if (!this.app.vault.getAbstractFileByPath(fail.path)) {
                         this.addToQueueTask(fail.peerId || null, { taskType: 'send-delete', path: fail.path });
                     } else {
                         this.failedSyncs.splice(i, 1);
                     }
                }
            }
        }
        if (changed) this.debouncedSaveState();
    }

    public reinitializeConnectionManager() {
        if (this.peerInitRetryTimeout) clearTimeout(this.peerInitRetryTimeout);
        if (this.clusterConnectionInterval) { clearInterval(this.clusterConnectionInterval); this.clusterConnectionInterval = null; }
        this.peer?.destroy();
        this.directIpClient?.stop();
        this.directIpServer?.stop();
        this.directIpClient = null;
        this.directIpServer = null;
        this.connections.clear();
        this.activeTransfers.clear();
        this.initializeConnectionManager();
    }

    initializeConnectionManager(onOpen?: (id: string) => void) {
        if (this.peerInitRetryTimeout) clearTimeout(this.peerInitRetryTimeout);
        
        if (!Platform.isMobile) {
            this.lanDiscovery.startBroadcasting(this.getMyPeerInfo());
            this.lanDiscovery.startListening();
        }

        if (this.getConnectionMode() === 'peerjs') {
            this.initializePeer(onOpen);
        } else {
            this.updateStatus();
        }
    }

    initializePeer(onOpen?: (id: string) => void) {
        if (this.peer && !this.peer.destroyed) {
            if (this.peer.disconnected) {
                // A disconnected (but not destroyed) peer can be revived without a full re-init.
                // Previously this branch silently did nothing, leaving sync offline after a
                // network change until the user reloaded the plugin.
                this.log('initializePeer: reviving disconnected peer via reconnect().');
                this.peer.reconnect();
            } else if (onOpen) {
                onOpen(this.peer.id);
            }
            return;
        }
        if (this.peerInitRetryTimeout) clearTimeout(this.peerInitRetryTimeout);
        this.peer?.destroy();
        this.updateStatus({ text: 'Connecting...', icon: 'plug', spin: true, state: 'loading' });

        let peerOptions: PeerJSOption = {};
        if (this.settings.useCustomPeerServer) { peerOptions = { ...this.settings.customPeerServerConfig }; }

        this.log(`Attempting to connect to PeerJS server (Attempt: ${this.peerInitAttempts + 1})...`);
        try {
            this.peer = new Peer(this.settings.deviceId, peerOptions);
        } catch (e) {
            this.handlePeerError(e);
            return;
        }

        const connectionTimeout = setTimeout(() => { this.log('PeerJS connection timed out.'); this.handlePeerError(new Error("Connection timed out")); }, 15000);

        this.peer.on('open', (id) => {
            clearTimeout(connectionTimeout);
            this.peerInitAttempts = 0;
            this.log(`PeerJS connection open. ID: ${id}`);
            this.showNotice(`Decentralized Sync network is online.`, 'verbose', 3000);
            this.updateStatus();
            this.tryToConnectToClusterPeers();
            if (!Platform.isMobile) {
                this.lanDiscovery.startBroadcasting(this.getMyPeerInfo());
            }
            onOpen?.(id);
        });

        this.peer.on('connection', (conn) => { this.log("Incoming PeerJS connection from:", conn.peer); this.setupConnection(conn); });
        this.peer.on('error', (err) => { clearTimeout(connectionTimeout); this.handlePeerError(err); });
        this.peer.on('disconnected', () => {
            this.showNotice('Sync network disconnected. Attempting to reconnect...', 'important');
            this.updateStatus({ text: 'Reconnecting...', icon: 'plug', spin: true, state: 'loading' });
            // Attempt lightweight reconnect first (Phase 3.1)
            if (this.peer && !this.peer.destroyed) {
                this.peer.reconnect();
                // Arm a fallback in case peer.reconnect() stalls silently
                if (this.peerReconnectFallbackTimeout !== null) clearTimeout(this.peerReconnectFallbackTimeout);
                this.peerReconnectFallbackTimeout = window.setTimeout(() => {
                    this.peerReconnectFallbackTimeout = null;
                    // If still disconnected after the window, fall through to full re-init
                    if (this.peer && this.peer.disconnected) {
                        this.log('PeerJS reconnect() stalled — falling back to full re-initialization.');
                        this.handlePeerError(new Error('Reconnect timed out'));
                    }
                }, 15000);
            }
        });
        this.peer.on('open', (id) => {
            // Cancel the reconnect fallback timer if the peer comes back online
            if (this.peerReconnectFallbackTimeout !== null) {
                clearTimeout(this.peerReconnectFallbackTimeout);
                this.peerReconnectFallbackTimeout = null;
            }
        });
        this.peer.on('close', () => { this.showNotice('Sync connection closed permanently.', 'important'); this.handlePeerError(new Error("Peer closed.")); });
    }

    private handlePeerError(err: any) {
        console.error("PeerJS Error:", err);
        
        if (err.type === 'peer-unavailable') {
            this.log("A requested peer is currently offline or unavailable.");
            return;
        }

        this.peer?.destroy();
        this.peer = null;
        this.connections.forEach(conn => conn.close());
        this.connections.clear();
        this.activeTransfers.clear();
    
        let userMessage = 'Connection Failed';
        switch(err.type) {
            case 'network': userMessage = 'Network Error. Check internet connection.'; break;
            case 'server-error': userMessage = 'Server Error. Try again later.'; break;
            case 'disconnected': userMessage = 'Disconnected from server.'; break;
        }
        this.updateStatus({ text: `Error: ${userMessage}`, icon: 'alert-triangle', state: 'error' });
    
        this.peerInitAttempts++;
        const backoff = Math.min(30000, this.peerInitAttempts * 2000);
        this.showNotice(`Sync connection failed. Retrying in ${backoff / 1000}s...`, 'info');
    
        if (this.peerInitRetryTimeout) clearTimeout(this.peerInitRetryTimeout);
        this.peerInitRetryTimeout = window.setTimeout(() => {
            this.updateStatus({ text: 'Retrying connection...', icon: 'refresh-cw', spin: true, state: 'loading' });
            this.initializePeer();
        }, backoff);
    }

    setupConnection(conn: DataConnection, pin?: string) {
        this.pendingConnections.add(conn.peer);
        conn.on('open', async () => {
            this.pendingConnections.delete(conn.peer);
            this.log("DataConnection open with:", conn.peer);
            
            const payload = { type: 'handshake', peerInfo: this.getMyPeerInfo(), pin };
            if (this.settings.peerKeys[conn.peer]) {
                try {
                    // encryptPayload already returns { type: 'encrypted', iv, data } —
                    // wrapping it again produced a payload the receiver could never
                    // decrypt (raw.iv/raw.data undefined), so every encrypted
                    // reconnect handshake was silently dropped.
                    const encrypted = await this.encryptPayload(payload, conn.peer);
                    conn.send(encrypted);
                } catch(e) {
                    this.log("Failed to encrypt handshake", e);
                    conn.send(payload);
                }
            } else {
                conn.send(payload);
            }
            // Role announcement is deferred to handleHandshake (after conn is in this.connections)
            // to avoid isTwoDeviceMode() seeing wrong connections.size
            this.resumeTransfers(conn.peer);
        });
        conn.on('data', async (raw: any) => {
            this.handleRawIncomingData(raw, conn).catch(e => {
                this.log("Unhandled error in incoming data listener", e);
            });
        });
        conn.on('close', () => {
            this.pendingConnections.delete(conn.peer);
            const peerId = conn.peer;
            this.log("DataConnection closed with:", peerId);
            this.connections.delete(peerId);
            this.lastHeard.delete(peerId);
            this.manualPingStart.delete(peerId);
            
            // Clear remote locks from this peer
            for (const [path, lock] of this.remoteLocks.entries()) {
                if (lock.peerId === peerId) this.remoteLocks.delete(path);
            }

            for (const [id, transfer] of this.activeTransfers.entries()) {
                if (transfer.peerId === peerId && transfer.direction === 'download') {
                    this.activeTransfers.delete(id);
                }
            }
            this.updateStatus();

            // Fix: Abort sync immediately if the connection to the syncing peer closes mid-sync
            if (this.syncState.isSyncing && this.syncState.peerId === peerId) {
                this.abortSync(new SyncError(SyncErrorCategory.CONNECTION_ERROR, "Connection closed mid-sync.", false, "Check peer connection."));
            }

            if (this.pendingAcks.size > 0) {
                for (const [id, ack] of this.pendingAcks.entries()) {
                    if (ack.peerId === peerId) {
                        ack.reject(new Error("Connection closed"));
                        this.pendingAcks.delete(id);
                    }
                }
            }
            this.log(`Peer disconnected: ${peerId}`);
            if (peerId === this.settings.companionPeerId) {
                this.showNotice(`Paired Device disconnected. Will try to reconnect automatically.`, 'important');
            }
            this.log("Connection closed, ensuring connection attempts continue.");
            this.tryToConnectToClusterPeers();
        });
        conn.on('error', (err) => { 
            this.pendingConnections.delete(conn.peer);
            console.error(`Connection error with ${conn.peer}:`, err); 
            this.showNotice(`Connection error with a peer.`, 'error'); 
        });
    }

    async handleRawIncomingData(raw: any, conn: DataConnection) {
        let data = raw;

        // A 2.x peer sends the old base64-in-JSON envelope. It is unreadable here and
        // the version gate in handleHandshake cannot fire (the handshake itself may be
        // encrypted), so name the cause instead of silently dropping every message.
        if (raw && raw.type === 'encrypted') {
            this.showNotice(
                'A peer is running an older, incompatible version of Obsidian Decentralized. Update it to sync.',
                'error', 10000
            );
            conn.close();
            return;
        }

        if (raw && raw.type === 'encrypted-frame') {
            if (this.settings.peerKeys[conn.peer]) {
                try {
                    data = await this.decryptPayload(raw, conn.peer);
                } catch(e) {
                    this.log("Decryption failed, ignoring message", e);
                    return;
                }
            } else if (this.activePsk) {
                // A peer pairing via the active QR code has no stored key yet. Adopt the
                // active PSK provisionally, and roll it back if it does not decrypt.
                this.settings.peerKeys[conn.peer] = this.activePsk;
                this.invalidateCryptoKey(conn.peer);
                try {
                    data = await this.decryptPayload(raw, conn.peer);
                    await this.saveSettings();
                    this.log(`Successfully authenticated new peer ${conn.peer} via active PSK`);
                } catch(e) {
                    delete this.settings.peerKeys[conn.peer];
                    this.invalidateCryptoKey(conn.peer);
                    this.log("Received encrypted message but no PSK found for peer, and active PSK failed", conn.peer);
                    return;
                }
            } else {
                this.log("Received encrypted message but no PSK found for peer", conn.peer);
                return;
            }
        }
        this.processIncomingData(data, conn);
    }
    
    async resumeTransfers(peerId: string) {
        const transfersToResume = Array.from(this.activeTransfers.values())
            .filter(t => t.peerId === peerId && t.status === 'paused' && t.direction === 'upload');

        for (const t of transfersToResume) {
            this.log(`Resuming transfer ${t.id} to ${peerId}`);
            t.status = 'active';
            this.updateStatus();
            
            const file = this.app.vault.getAbstractFileByPath(t.path);
            if (file instanceof TFile) {
                let content: ArrayBuffer;
                if (t.compressed) {
                    const textContent = await this.app.vault.read(file);
                    content = compressText(textContent);
                } else {
                    content = await this.app.vault.readBinary(file);
                }
                
                const ackPromise = new Promise<void>((resolve, reject) => {
                    const timeout = setTimeout(() => reject(new Error(`Transfer ${t.id} timed out`)), 60000);
                    this.pendingAcks.set(t.id, {
                        resolve: () => { clearTimeout(timeout); resolve(); },
                        reject: (e) => { clearTimeout(timeout); reject(e); },
                        peerId: peerId
                    });
                });

                try {
                    const vv = this.twoDeviceState.fileVersions[t.path];
                    await this.sendFileInChunks(t.peerId, t.path, file.stat.mtime, content, t.id, t.processedChunks, t.compressed, vv);
                    await ackPromise;
                    this.log(`Resumed transfer ${t.id} completed.`);
                    this.activeTransfers.delete(t.id);
                    this.debouncedSaveState();
                } catch (e) {
                    if (e.message === 'Paused') this.log(`Transfer ${t.id} paused again.`);
                    else this.log(`Resumed transfer ${t.id} failed:`, e);
                } finally {
                    if (this.pendingAcks.has(t.id)) {
                        this.pendingAcks.get(t.id)!.resolve();
                        this.pendingAcks.delete(t.id);
                    }
                }
            } else {
                this.activeTransfers.delete(t.id);
                this.debouncedSaveState();
            }
        }
    }

    startHeartbeat() {
        this.registerInterval(window.setInterval(() => {
            const now = Date.now();
            this.connections.forEach((conn, peerId) => {
                if (conn.open) {
                    // Send directly to bypass the sync queue
                    conn.send({ type: 'ping' });
                    const last = this.lastHeard.get(peerId);
                    if (last && now - last > 20000) { // Increased timeout to 20 seconds
                        this.log(`Peer ${peerId} timed out (Heartbeat).`);
                        conn.close();
                    }
                }
            });
        }, 5000)); // Check every 5 seconds
    }
    
    startSyncKeepAlive() {
        if (this.syncKeepAliveInterval) clearInterval(this.syncKeepAliveInterval);
        this.syncState.missedPings = 0;
        this.syncKeepAliveInterval = window.setInterval(() => {
            if (this.syncState.isSyncing && this.syncState.peerId) {
                if (this.syncState.missedPings >= 2) {
                    this.abortSync(new SyncError(SyncErrorCategory.CONNECTION_ERROR, "Peer stopped responding to pings.", false, "Check peer network connection."));
                    return;
                }
                this.syncState.missedPings++;
                const conn = this.connections.get(this.syncState.peerId);
                if (conn && conn.open) conn.send({ type: 'sync-ping' });
            } else {
                if (this.syncKeepAliveInterval) { clearInterval(this.syncKeepAliveInterval); this.syncKeepAliveInterval = null; }
            }
        }, 10000);
    }

    async processIncomingData(data: any, conn: DataConnection | null) {
        if (!data || !data.type) return;
        // Unwrap JSON-encoded sync control messages (see sendSyncMessage for why)
        if (data.type === 'sync-control-json' && data.jsonPayload) {
            try { data = JSON.parse(data.jsonPayload); } catch (e) { this.log('Failed to parse sync-control-json payload:', e); return; }
        }
        this.log("Received data:", data.type, "from", conn?.peer);
        if (conn?.peer) {
            this.lastHeard.set(conn.peer, Date.now());
            this.lastSuccessfulMessageTime.set(conn.peer, Date.now());
        }

        if (data.messageId && data.type !== 'sync-ack' && conn) {
            conn.send({ type: 'sync-ack', messageId: data.messageId });
            // Dedup: sendSyncMessage retries after 30s even if the first copy was merely
            // slow — re-processing a control message (e.g. request-batch) corrupts sync
            // state, so ack duplicates but process each messageId only once.
            if (this.processedMessageIds.has(data.messageId)) {
                this.log(`Ignoring duplicate delivery of ${data.type} (${data.messageId})`);
                return;
            }
            this.processedMessageIds.add(data.messageId);
            if (this.processedMessageIds.size > 500) {
                const oldest = this.processedMessageIds.values().next().value;
                if (oldest) this.processedMessageIds.delete(oldest);
            }
        }
        if (data.type === 'sync-ack') {
            const ack = this.pendingSyncAcks.get(data.messageId);
            if (ack) { ack.resolve(); this.pendingSyncAcks.delete(data.messageId); }
            return;
        }
        
        try {
            switch (data.type) {
                case 'handshake': this.handleHandshake(data, conn!); break;
                case 'role-announcement': 
                    if (this.isTwoDeviceMode()) {
                        this.log(`Role announcement from ${data.deviceId}: ${data.role}`);
                        // Validation: my role should be opposite
                        if (data.role === this.currentRole) {
                            this.log(`Role conflict detected! Re-evaluating.`);
                            this.currentRole = this.getMyRole(data.deviceId);
                        }
                    }
                    break;
                case 'cluster-gossip': this.handleClusterGossip(data); break;
                case 'companion-pair': this.handleCompanionPair(data); break;
                case 'ack':
                    if (this.pendingAcks.has(data.transferId)) {
                        this.log(`Ack received for ${data.transferId}.`);
                        this.pendingAcks.get(data.transferId)!.resolve();
                        this.pendingAcks.delete(data.transferId);
                        this.resetIdleTimeout();
                    }
                    break;
                case 'nack':
                    if (this.pendingAcks.has(data.transferId)) {
                        this.log(`Nack received for ${data.transferId} (Reason: ${data.reason}).`);
                        this.pendingAcks.get(data.transferId)!.reject(new Error(`IntegrityError: ${data.reason}`));
                        this.pendingAcks.delete(data.transferId);
                        this.resetIdleTimeout();
                    }
                    break;
                case 'file-update': 
                    this.applyFileUpdate(data).then(() => {
                        if (conn && data.transferId && !data.skipAck) conn.send({ type: 'ack', transferId: data.transferId });
                        this.resetIdleTimeout();
                    }).catch(e => {
                        this.log(`Failed to apply file update: ${data.path}`, e);
                        if (conn && data.transferId && !data.skipAck) {
                            const reason = (e instanceof Error && e.message.includes('IntegrityError')) ? 'integrity-failure' : 'write-error';
                            conn.send({ type: 'nack', transferId: data.transferId, reason });
                        }
                    }); 
                    break;
                case 'file-batch-binary':
                    this.applyFileBatchBinary(data).then((results) => {
                        this.resetIdleTimeout();
                        // NOTE: do NOT send 'batch-complete' from here. The batchId belongs to
                        // OUR pull batch — echoing it back would be misread by the sender's
                        // handleBatchComplete as completion of ITS OWN pull batch, corrupting
                        // its sync state. The sender emits the authoritative batch-complete
                        // via recordBatchTaskCompletion once all its tasks finish.
                        if (results.failed.length > 0) {
                            this.log(`Batch ${data.batchId}: failed to apply ${results.failed.length} file(s):`, results.failed);
                        }
                    }).catch(e => {
                        this.log(`Critical failure unpacking file batch`, e);
                    });
                    break;
                case 'file-delta':
                    this.applyFileDelta(data).then(() => {
                        if (conn && data.transferId) conn.send({ type: 'ack', transferId: data.transferId });
                        this.resetIdleTimeout();
                    }).catch(e => {
                        this.log(`Failed to apply delta: ${data.path}`, e);
                        if (conn && data.transferId) {
                            const reason = (e instanceof Error && e.message.includes('IntegrityError')) ? 'integrity-failure' : 'write-error';
                            conn.send({ type: 'nack', transferId: data.transferId, reason });
                        }
                    });
                    break;
                case 'file-delete': this.applyFileDelete(data); break;
                case 'file-rename': this.applyFileRename(data); break;
                case 'folder-create': this.applyFolderCreate(data); break;
                case 'folder-delete': this.applyFolderDelete(data); break;
                case 'folder-rename': this.applyFolderRename(data); break;
                
                // Pull-based Sync
                case 'request-full-sync': await this.handleFullSyncRequest(data, conn!); break;
                case 'sync-plan': await this.handleSyncPlan(data, conn!); break;
                case 'request-batch': await this.handleRequestBatch(data, conn!); break;
                case 'batch-complete': this.handleBatchComplete(data, conn!); break;
                
                case 'full-sync-complete': 
                    this.peerSyncComplete.set(conn!.peer, true);
                    this.checkFullSyncCompletion(conn!.peer);
                    break;
                case 'request-file': this.handleRequestFile(data, conn!); break;
                case 'file-chunk-start': this.handleFileChunkStart(data, conn); break;
                case 'file-chunk-data': await this.handleFileChunkData(data, conn!); break;
                
                case 'ping': conn?.send({ type: 'pong' }); break;
                case 'pong': 
                    if (this.manualPingStart.has(conn!.peer)) {
                        const start = this.manualPingStart.get(conn!.peer)!;
                        const rtt = Date.now() - start;
                        this.manualPingStart.delete(conn!.peer);
                        this.showNotice(`Ping to ${this.clusterPeers.get(conn!.peer)?.friendlyName || conn!.peer}: ${rtt}ms`, 'important');
                    }
                    break;
                case 'sync-ping': conn?.send({ type: 'sync-pong' }); this.resetIdleTimeout(); break;
                case 'sync-pong': this.syncState.missedPings = 0; this.resetIdleTimeout(); break;
                    
                case 'cluster-forget': this.handleClusterForget(data); break;
                case 'cluster-kick': this.handleClusterKick(data); break;
                case 'cluster-rename': this.handleClusterRename(data); break;
                
                // Locking
                case 'lock-request': this.handleLockRequest(data, conn!); break;
                case 'lock-grant': this.handleLockGrant(data); break;
                case 'lock-deny': this.handleLockDeny(data); break;
                case 'lock-release': this.handleLockRelease(data, conn!); break;
                
                // Editor Sync
                case 'editor-active': this.handleEditorActive(data, conn!); break;
                case 'editor-delta': this.handleEditorDelta(data); break;
                
                // Merkle
                case 'merkle-root': await this.handleMerkleRoot(data, conn!); break;
                case 'merkle-node-request': await this.handleMerkleNodeRequest(data, conn!); break;
                case 'merkle-node-response': await this.handleMerkleNodeResponse(data, conn!); break;
            }
        } catch (e) {
            this.log(`Error processing incoming data (type: ${data.type}):`, e);
            if (this.syncState.isSyncing && (data.type === 'request-full-sync' || data.type === 'sync-plan' || data.type === 'request-batch')) {
                this.abortSync(new SyncError(SyncErrorCategory.PROTOCOL_ERROR, `Sync protocol error: ${e instanceof Error ? e.message : String(e)}`, false, "Check logs."));
            }
        }
    }

    handleHandshake(data: HandshakePayload, conn: DataConnection) {
        if (this.joinPin && data.pin !== this.joinPin) { 
            this.showNotice(`Incorrect PIN from ${data.peerInfo.friendlyName}. Connection rejected.`, 'error', 10000); 
            conn.close(); 
            return; 
        }
        if (this.joinPin && !this.settings.requirePinForAllConnections) { 
            this.joinPin = null; 
        } 
        this.showNotice(`Connected to ${data.peerInfo.friendlyName}`, 'important', 4000);
        this.lastHeard.set(conn.peer, Date.now());
        this.connections.set(conn.peer, conn); this.clusterPeers.set(conn.peer, data.peerInfo); this.updateStatus();
        this.saveKnownPeers();
        const existingPeers = Array.from(this.clusterPeers.values());
        
        if (this.getConnectionMode() === 'direct-ip') {
            if (!data.isResponse) {
                this.sendData(conn.peer, { type: 'handshake', peerInfo: this.getMyPeerInfo(), pin: data.pin, isResponse: true } as any);
            }
        } else {
            this.sendData(conn.peer, { type: 'cluster-gossip', peers: existingPeers });
            this.broadcastData({ type: 'cluster-gossip', peers: [this.getMyPeerInfo(), data.peerInfo] });
        }
        
        if (this.isTwoDeviceMode()) {
            this.currentRole = this.getMyRole(conn.peer);
            this.sendData(conn.peer, { type: 'role-announcement', role: this.currentRole, deviceId: this.settings.deviceId });

            // Auto-reconciliation: the primary initiates a cheap Merkle-root exchange so
            // paired vaults converge automatically after a reconnect. Without this trigger
            // the whole Merkle diffing path was dead code — nothing ever sent 'merkle-root'.
            if (this.settings.enableTwoDeviceOptimizations && this.currentRole === 'primary' && !this.syncState.isSyncing) {
                this.buildMerkleTree()
                    .then(tree => this.sendData(conn.peer, { type: 'merkle-root', rootHash: tree.hash }))
                    .catch(e => this.log('Failed to build Merkle tree for auto-reconciliation', e));
            }
        }
    }

    handleClusterGossip(data: ClusterGossipPayload) {
        if (this.getConnectionMode() !== 'peerjs') return;
        let hasNew = false;
        data.peers.forEach(peerInfo => {
            if (peerInfo.deviceId === this.settings.deviceId || this.connections.has(peerInfo.deviceId)) return;
            if (!this.clusterPeers.has(peerInfo.deviceId)) {
                this.clusterPeers.set(peerInfo.deviceId, peerInfo);
                hasNew = true;
            }
        });
        if (hasNew) {
            this.saveKnownPeers();
            this.updateStatus();
            this.tryToConnectToClusterPeers();
        }
    }

    async handleCompanionPair(data: CompanionPairPayload) {
        this.settings.companionPeerId = data.peerInfo.deviceId; await this.saveSettings();
        this.showNotice(`Paired with ${data.peerInfo.friendlyName} as a primary sync partner.`, 'important', 4000);
        this.tryToConnectToClusterPeers();
    }

    tryToConnectToClusterPeers() {
        if (this.getConnectionMode() !== 'peerjs') return;
        
        const attemptConnection = () => {
            if (!this.peer || this.peer.disconnected) return;
            
            const connectToPeer = (peerId: string) => {
                if (peerId === this.settings.deviceId) return;
                if (this.connections.has(peerId) || this.pendingConnections.has(peerId)) return;
                
                this.log(`Attempting to connect to cluster peer ${peerId}`); 
                this.pendingConnections.add(peerId);
                const conn = this.peer!.connect(peerId, { reliable: true }); 
                if (conn) {
                    this.setupConnection(conn);
                    setTimeout(() => {
                        if (this.pendingConnections.has(peerId)) {
                            this.pendingConnections.delete(peerId);
                            this.log(`Pending connection to ${peerId} timed out. Removing from pending set.`);
                        }
                    }, 15000);
                } else {
                    this.pendingConnections.delete(peerId);
                }
            };

            const companionId = this.settings.companionPeerId;
            if (companionId) connectToPeer(companionId);

            for (const peerId of this.clusterPeers.keys()) {
                if (peerId !== companionId) connectToPeer(peerId);
            }
        };
        
        attemptConnection();
        if (!this.clusterConnectionInterval) {
            this.clusterConnectionInterval = window.setInterval(attemptConnection, COMPANION_RECONNECT_INTERVAL_MS); 
            // Fix: Do not register this dynamically recreated interval to avoid leaking in Obsidian core's internal list.
        }
    }

    /**
     * Centralized handler for all network-change signals (Phase 3.2).
     * Triggered by: browser `online`/`offline` events, and the discovery
     * `network-change` event (emitted after the LAN multicast socket restarts).
     */
    private handleNetworkChange() {
        this.log('Network change detected — checking transports...');
        const mode = this.getConnectionMode();

        if (mode === 'peerjs') {
            if (!this.peer || this.peer.destroyed || this.peer.disconnected) {
                this.log('Network change: re-initializing PeerJS peer.');
                this.initializePeer();
            } else {
                // Peer is alive; just ensure cluster peers are reconnected
                this.tryToConnectToClusterPeers();
            }
        } else if (mode === 'direct-ip') {
            if (this.directIpClient) {
                this.log('Network change: triggering DirectIpClient reconnect.');
                this.directIpClient.triggerReconnect();
            }
        }
    }

    handleClusterForget(data: ClusterForgetPayload) {
        this.log(`Received instruction to forget device: ${data.targetDeviceId}`);
        this.pendingConnections.delete(data.targetDeviceId);
        if (this.connections.has(data.targetDeviceId)) {
            this.connections.get(data.targetDeviceId)?.close();
        }
        this.clusterPeers.delete(data.targetDeviceId);
        this.saveKnownPeers();
        this.updateStatus();
    }

    handleClusterKick(data: ClusterKickPayload) {
        if (data.targetDeviceId === this.settings.deviceId) {
            this.showNotice("You have been kicked from the cluster.", 'error');
        } else if (this.connections.has(data.targetDeviceId)) {
            this.log(`Kicking device: ${data.targetDeviceId}`);
            this.connections.get(data.targetDeviceId)?.close();
        }
    }

    handleClusterRename(data: ClusterRenamePayload) {
        if (data.targetDeviceId === this.settings.deviceId) {
            this.settings.friendlyName = data.newName;
            this.saveSettings();
            this.showNotice(`Your device was renamed to ${data.newName} by the cluster.`, 'info');
        }
        const peer = this.clusterPeers.get(data.targetDeviceId);
        if (peer) {
            peer.friendlyName = data.newName;
            this.saveKnownPeers();
            this.updateStatus();
        }
    }

    public async forgetCompanion() {
        const companionId = this.settings.companionPeerId;
        if (companionId) { 
            const conn = this.connections.get(companionId); 
            conn?.close(); 
            this.pendingConnections.delete(companionId);
        }
        this.settings.companionPeerId = undefined; await this.saveSettings(); 
        this.showNotice('Paired Device link forgotten.', 'important', 3000);
    }

    private static textEncoder = new TextEncoder();
    private static textDecoder = new TextDecoder();
    private static hexTable: string[] = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, '0'));

    private async getHash(buffer: ArrayBuffer | string): Promise<string> {
        const data = typeof buffer === 'string' ? ObsidianDecentralizedPlugin.textEncoder.encode(buffer) : buffer;
        const hashBuffer = await window.crypto.subtle.digest('SHA-256', data);
        const hashArray = new Uint8Array(hashBuffer);
        return Array.from(hashArray).map(b => ObsidianDecentralizedPlugin.hexTable[b]).join('');
    }

    // --- Locking Handlers ---
    async requestLock(path: string): Promise<boolean> {
        if (!this.isTwoDeviceMode() || !this.twoDevicePeerId) return true;
        const requestId = this.generateTransferId(path);
        
        return new Promise((resolve) => {
            const timeout = window.setTimeout(() => {
                this.pendingLockRequests.delete(requestId);
                resolve(false);
            }, 5000);
            
            this.pendingLockRequests.set(requestId, { resolve, timeout });
            this.sendData(this.twoDevicePeerId!, { type: 'lock-request', path, requestId });
        });
    }

    handleLockRequest(data: LockRequestPayload, conn: DataConnection) {
        const path = data.path;
        const view = this.app.workspace.getActiveViewOfType(MarkdownView);
        const isEditing = view && view.file && view.file.path === path;
        
        if (isEditing || this.heldLocks.has(path)) {
            this.sendData(conn.peer, { type: 'lock-deny', path, requestId: data.requestId, reason: 'File is actively being edited' });
        } else {
            const expiresAt = Date.now() + LOCK_EXPIRATION_MS;
            this.remoteLocks.set(path, { peerId: conn.peer, expiresAt });
            this.sendData(conn.peer, { type: 'lock-grant', path, requestId: data.requestId, grantedUntil: expiresAt });
        }
    }

    handleLockGrant(data: LockGrantPayload) {
        if (this.pendingLockRequests.has(data.requestId)) {
            const req = this.pendingLockRequests.get(data.requestId)!;
            clearTimeout(req.timeout);
            this.heldLocks.set(data.path, { peerId: this.twoDevicePeerId!, expiresAt: data.grantedUntil });
            req.resolve(true);
            this.pendingLockRequests.delete(data.requestId);
        }
    }

    handleLockDeny(data: LockDenyPayload) {
        if (this.pendingLockRequests.has(data.requestId)) {
            const req = this.pendingLockRequests.get(data.requestId)!;
            clearTimeout(req.timeout);
            req.resolve(false);
            this.pendingLockRequests.delete(data.requestId);
            this.showNotice(`Lock denied for ${data.path}: ${data.reason}`, 'warning');
        }
    }

    handleLockRelease(data: LockReleasePayload, conn: DataConnection) {
        if (this.remoteLocks.has(data.path) && this.remoteLocks.get(data.path)!.peerId === conn.peer) {
            this.remoteLocks.delete(data.path);
        }
    }
    
    cleanupLocks() {
        const now = Date.now();
        for (const [path, lock] of this.heldLocks.entries()) {
            if (now > lock.expiresAt) {
                this.heldLocks.delete(path);
                if (this.twoDevicePeerId) this.sendData(this.twoDevicePeerId, { type: 'lock-release', path });
            }
        }
        for (const [path, lock] of this.remoteLocks.entries()) {
            if (now > lock.expiresAt) {
                this.remoteLocks.delete(path);
            }
        }
    }

    // --- Editor Sync Handlers ---
    handleEditorActive(data: EditorActivatePayload, conn: DataConnection) {
        this.activeEditorLocks.set(data.path, conn.peer);
        this.showNotice(`Peer is actively editing ${data.path}`, 'info', 3000);
    }

    handleEditorDelta(data: EditorDeltaPayload) {
        const view = this.app.workspace.getActiveViewOfType(MarkdownView);
        if (view && view.file && view.file.path === data.path) {
            const cm = (view as any).editor?.cm;
            if (cm) {
                this.isApplyingRemoteEdit = true;
                this.ignoreNextEventForPath(data.path);
                
                const currentText = view.editor.getValue();
                const dmp = new DiffMatchPatch();
                const patches = dmp.patch_fromText(data.patches);
                const [newText, results] = dmp.patch_apply(patches, currentText);
                
                if (results.every((r: boolean) => r === true)) {
                    const diff = dmp.diff_main(currentText, newText);
                    dmp.diff_cleanupSemantic(diff);
                    let offset = 0;
                    const changes: Array<{ from: number, to?: number, insert?: string }> = [];
                    for (let i = 0; i < diff.length; i++) {
                        const [op, text] = diff[i];
                        if (op === 0) {
                            offset += text.length;
                        } else if (op === -1) {
                            const nextDiff = diff[i + 1];
                            if (nextDiff && nextDiff[0] === 1) {
                                changes.push({ from: offset, to: offset + text.length, insert: nextDiff[1] });
                                offset += text.length;
                                i++; // skip insert
                            } else {
                                changes.push({ from: offset, to: offset + text.length });
                                offset += text.length;
                            }
                        } else if (op === 1) {
                            changes.push({ from: offset, insert: text });
                        }
                    }
                    
                    const tx: any = { changes };
                    const syncAnnotation = (window as any).CM_Annotation ? (window as any).CM_Annotation.define() : null;
                    if (syncAnnotation) tx.annotations = syncAnnotation.of('remote-sync');
                    
                    cm.dispatch(tx);
                    
                    this.lastSentContent.set(data.path, { content: newText, timestamp: Date.now() });
                }
                
                setTimeout(() => this.isApplyingRemoteEdit = false, 50);
            }
        }
    }

    async loadQueueState() {
        const queuePath = `${this.manifest.dir}/queue.json`;
        try {
            if (await this.app.vault.adapter.exists(queuePath)) {
                const data = await this.app.vault.adapter.read(queuePath);
                const items = JSON.parse(data);
                if (this.queueManager && Array.isArray(items)) {
                    this.queueManager.loadQueue(items);
                }
            }
        } catch (e) {
            console.error('Failed to load queue state:', e);
        }
    }

    async saveQueueState() {
        if (!this.queueManager) return;
        const queuePath = `${this.manifest.dir}/queue.json`;
        const tmpPath = queuePath + '.tmp';
        try {
            const queueState = this.queueManager.getQueue();
            const json = JSON.stringify(queueState);
            await this.app.vault.adapter.write(tmpPath, json);
            await this.app.vault.adapter.write(queuePath, json);
            await this.app.vault.adapter.remove(tmpPath);
        } catch (e) {
            console.error('Failed to save queue state:', e);
        }
    }

    async sendFileUpdate(file: TFile, peerId?: string, forceFull: boolean = false) {
        if (!this.isPathSyncable(file.path)) return;
        const isBinaryFile = this.isBinary(file.extension);
        if (isBinaryFile && !this.shouldSyncAllFileTypes()) { this.log(`Skipping binary file because 'syncAllFileTypes' is disabled: ${file.path}`); return; }
        if (!isBinaryFile && !this.shouldSyncAllFileTypes()) { const textWhitelist = ['md', 'css', 'js', 'json']; if (!textWhitelist.includes(file.extension)) { this.log(`Skipping non-whitelisted text file: ${file.path}`); return; } }
        this.log(`Queueing file update for ${peerId || 'broadcast'}: ${file.path}`);
        
        this.addToQueueTask(peerId || null, { taskType: 'send-file', path: file.path, mtime: file.stat.mtime, forceFull });
    }

    async sendFileInChunks(peerId: string, path: string, mtime: number, fileContent: ArrayBuffer, transferId: string, startIndex = 0, compressed?: boolean, versionVector?: VersionVector) {
        const isDirectIp = this.getConnectionMode() === 'direct-ip';
        let conn: DataConnection | undefined;

        if (!isDirectIp) {
            conn = this.connections.get(peerId);
            if (!conn?.open) {
                this.log(`No open connection to ${peerId} to send chunks. Aborting transfer.`);
                this.pendingAcks.get(transferId)?.reject(new Error("Connection closed"));
                return;
            }
        }
        
        const chunkSize = this.getChunkSize();
        const existingTransfer = this.activeTransfers.get(transferId);
        this.activeTransfers.set(transferId, existingTransfer || {
            id: transferId,
            path,
            direction: 'upload',
            peerId,
            totalChunks: Math.ceil(fileContent.byteLength / chunkSize),
            processedChunks: startIndex,
            startTime: Date.now(),
            lastUpdate: Date.now(),
            status: 'active',
            chunkSize: chunkSize,
            compressed: compressed
        });
        this.updateStatus();
        this.debouncedSaveState();

        const totalChunks = Math.ceil(fileContent.byteLength / chunkSize);
        this.log(`Sending file in ${totalChunks} chunks to ${peerId}: ${path} (ID: ${transferId})`);
        
        let chunkHash = '';
        try { chunkHash = await this.getHash(fileContent); } catch(e) {}

        const YIELD_THRESHOLD_MS = 100;
        let lastYieldTime = Date.now();
        const transferStartTime = Date.now();
        
        if (startIndex === 0) {
            const startPayload: FileChunkStartPayload = { type: 'file-chunk-start', path, mtime, totalChunks, transferId, fileHash: chunkHash, compressed, versionVector };
            if (isDirectIp) {
                let encPayload: any = startPayload;
                if (this.settings.enableEncryption && this.settings.peerKeys[peerId]) encPayload = await this.encryptPayload(startPayload, peerId);
                if (this.directIpClient) this.directIpClient.send(encPayload);
                else if (this.directIpServer) this.directIpServer.sendTo(peerId, encPayload);
            }
            else {
                let encPayload: any = startPayload;
                if (this.settings.enableEncryption && this.settings.peerKeys[peerId]) encPayload = await this.encryptPayload(startPayload, peerId);
                conn!.send(encPayload);
            }
            this.resetIdleTimeout();
        }
        
        for (let i = startIndex; i < totalChunks; i++) {
            if (!this.activeTransfers.has(transferId)) {
                throw new Error("Transfer cancelled or timed out");
            }
            if (isDirectIp) {
                const clientConnected = this.directIpClient && this.directIpClient.isOpen;
                const serverHasPeer = this.directIpServer && this.directIpServer.hasClient(peerId);
                if (!clientConnected && !serverHasPeer) {
                    this.log(`Direct IP Connection closed mid-transfer. Pausing.`);
                    const t = this.activeTransfers.get(transferId);
                    if (t) { t.status = 'paused'; t.lastUpdate = Date.now(); }
                    this.updateStatus();
                    this.debouncedSaveState();
                    throw new Error("Paused");
                }
            } else if (!conn!.open) {
                this.log(`Connection to ${peerId} closed mid-transfer. Pausing.`);
                const t = this.activeTransfers.get(transferId);
                if (t) { t.status = 'paused'; t.lastUpdate = Date.now(); }
                this.updateStatus();
                this.debouncedSaveState();
                throw new Error("Paused");
            }
            try {
                const start = i * chunkSize; const end = start + chunkSize; const chunk = fileContent.slice(start, end);
                const chunkPayload: FileChunkDataPayload = { type: 'file-chunk-data', transferId, index: i, data: chunk };
                
                let encPayload: any = chunkPayload;
                if (this.settings.enableEncryption && this.settings.peerKeys[peerId]) encPayload = await this.encryptPayload(chunkPayload, peerId);

                this.syncState.bytesTransferred += chunk.byteLength;

                if (isDirectIp) {
                    if (this.directIpClient) this.directIpClient.send(encPayload);
                    else if (this.directIpServer) this.directIpServer.sendTo(peerId, encPayload);

                    const getBuffer = () => this.directIpClient ? this.directIpClient.getBufferedAmount() : this.directIpServer!.getBufferedAmount(peerId);
                    if (getBuffer() > 1024 * 1024 * 32) {
                        await new Promise<void>(resolve => {
                            const check = () => {
                                if (getBuffer() <= 1024 * 1024 * 16) resolve();
                                else if (typeof setImmediate !== 'undefined') setImmediate(check);
                                else setTimeout(check, 1);
                            };
                            check();
                        });
                    }
                }
                else {
                    conn!.send(encPayload);
                    const dc = (conn! as any).dataChannel || (conn! as any)._dc;
                    if (dc && dc.bufferedAmount > 1024 * 1024 * 16) {
                        await new Promise<void>(resolve => {
                            if (dc.bufferedAmount <= 1024 * 1024 * 8) return resolve();
                            const oldHandler = dc.onbufferedamountlow;
                            dc.bufferedAmountLowThreshold = 1024 * 1024 * 8;
                            dc.onbufferedamountlow = () => {
                                dc.onbufferedamountlow = oldHandler;
                                resolve();
                            };
                        });
                    }
                }

                const transfer = this.activeTransfers.get(transferId);
                if (transfer) {
                    transfer.processedChunks = i + 1;
                    transfer.lastUpdate = Date.now();
                }
                
                const now = Date.now();
                if (now - lastYieldTime > YIELD_THRESHOLD_MS) {
                    this.updateStatus();
                    await new Promise(resolve => setTimeout(resolve, 0));
                    lastYieldTime = Date.now();
                }
                
                if (i % 100 === 0) this.debouncedSaveState();
                this.resetIdleTimeout();
            } catch (e) {
                this.log(`Error sending chunk ${i} for ${path}. Aborting.`, e);
                throw e;
            }
        }
        this.recordTransferSample(fileContent.byteLength, Date.now() - transferStartTime);
        this.log(`Finished sending all chunks for ${path} to ${peerId}. Waiting for ack.`);
    }

    handleFileChunkStart(payload: FileChunkStartPayload, conn: DataConnection | null) {
        // Guard: reject transfer if claimed total size exceeds the 512 MB reassembly cap
        const MAX_REASSEMBLY_SIZE = 512 * 1024 * 1024;
        if (payload.totalChunks > MAX_REASSEMBLY_SIZE / MAX_CHUNK_SIZE) {
            this.log(`Rejecting chunked transfer for ${payload.path}: totalChunks (${payload.totalChunks}) would exceed max reassembly size.`);
            return;
        }
        this.pendingFileChunks.set(payload.transferId, { path: payload.path, mtime: payload.mtime, chunks: new Array(payload.totalChunks), total: payload.totalChunks, receivedCount: 0, lastUpdated: Date.now(), fileHash: payload.fileHash || '', compressed: payload.compressed, versionVector: payload.versionVector }); 
        this.activeTransfers.set(payload.transferId, {
            id: payload.transferId,
            path: payload.path,
            direction: 'download',
            peerId: conn?.peer || 'Direct-IP',
            totalChunks: payload.totalChunks,
            processedChunks: 0,
            startTime: Date.now(),
            lastUpdate: Date.now(),
            status: 'active'
        });
        this.resetIdleTimeout();
        this.log(`Receiving chunked file: ${payload.path}, ID: ${payload.transferId}`); 
    }

    async handleFileChunkData(payload: FileChunkDataPayload, conn: DataConnection) {
        const transfer = this.pendingFileChunks.get(payload.transferId); if (!transfer) { this.log("Received chunk for unknown transfer:", payload.transferId); return; }
        // Fix: Validate chunk size does not exceed MAX_CHUNK_SIZE to prevent OOM / heap memory allocation exploits
        if (payload.data.byteLength > MAX_CHUNK_SIZE) {
            this.log(`Received chunk exceeding MAX_CHUNK_SIZE (${payload.data.byteLength} bytes). Aborting transfer.`);
            this.pendingFileChunks.delete(payload.transferId);
            this.activeTransfers.delete(payload.transferId);
            return;
        }
        if (payload.index < 0 || payload.index >= transfer.total) {
            this.log(`Received invalid chunk index ${payload.index} for transfer ${payload.transferId}`);
            return;
        }
        if (!transfer.chunks[payload.index]) {
            transfer.chunks[payload.index] = payload.data;
            transfer.receivedCount++;
            this.syncState.bytesTransferred += payload.data.byteLength;
        }
        transfer.lastUpdated = Date.now();
        const active = this.activeTransfers.get(payload.transferId);
        if (active) { active.processedChunks = transfer.receivedCount; active.lastUpdate = Date.now(); }
        this.resetIdleTimeout();
        
        if (transfer.receivedCount === transfer.total) {
            this.log(`All chunks received for ${transfer.path}. Reassembling...`);
            this.pendingFileChunks.delete(payload.transferId);
            this.activeTransfers.delete(payload.transferId);
            
            const totalSize = transfer.chunks.reduce((sum, chunk) => sum + (chunk ? chunk.byteLength : 0), 0); 
            const reassembled = new Uint8Array(totalSize); 
            let offset = 0;
            for (let i = 0; i < transfer.chunks.length; i++) { 
                const chunk = transfer.chunks[i];
                if (chunk) { 
                    reassembled.set(new Uint8Array(chunk), offset); 
                    offset += chunk.byteLength; 
                }
                transfer.chunks[i] = null as any; 
            }
            
            try {
                const computedHash = await this.getHash(reassembled.buffer);
                if (transfer.fileHash && computedHash && transfer.fileHash !== computedHash) {
                    this.log(`Integrity check failed for chunked transfer ${transfer.path}. Rejecting.`);
                    this.sendData(conn.peer, { type: 'nack', transferId: payload.transferId, reason: 'integrity-failure' });
                    return;
                }

                await this.applyFileUpdate({ type: 'file-update', path: transfer.path, content: reassembled.buffer, mtime: transfer.mtime, encoding: 'binary', transferId: payload.transferId, compressed: transfer.compressed, versionVector: transfer.versionVector });
                this.sendData(conn.peer, { type: 'ack', transferId: payload.transferId }); this.log(`Reassembly complete for ${transfer.path}, sent ack.`);
            } catch (e) {
                this.log(`Failed to apply chunked file update: ${transfer.path}`, e);
                if (e instanceof Error && e.message.includes('IntegrityError')) {
                    this.sendData(conn.peer, { type: 'nack', transferId: payload.transferId, reason: 'integrity-failure' });
                } else {
                    this.sendData(conn.peer, { type: 'nack', transferId: payload.transferId, reason: 'write-error' });
                }
            }
        }
    }

    cleanupPendingChunks() {
        const now = Date.now();
        let statusChanged = false;

        for (const [id, transfer] of this.pendingFileChunks.entries()) {
            if (now - transfer.lastUpdated > 60000 * 5) { 
                this.log(`Cleaning up stale chunk transfer: ${id}`);
                this.pendingFileChunks.delete(id);
            }
        }
        for (const [id, transfer] of this.activeTransfers.entries()) {
            if (transfer.status === 'paused') continue;
            if (now - transfer.lastUpdate > 60000) { 
                this.log(`Cleaning up stale active transfer: ${id}`);
                this.activeTransfers.delete(id);
                // Reject (not resolve) the pending ACK — resolving would falsely signal success to the sender
                if (this.pendingAcks.has(id)) {
                    this.pendingAcks.get(id)!.reject(new Error('Transfer timed out and was cleaned up'));
                    this.pendingAcks.delete(id);
                }
                statusChanged = true;
            }
        }
        
        this.pruneTombstones();

        // Evict stale and excess entries from lastSentContent cache (moved here from the per-item hot path)
        const MAX_SENT_CONTENT_CACHE = 200;
        const contentCacheTtl = 10 * 60 * 1000; // 10 minutes
        for (const [p, cacheData] of this.lastSentContent.entries()) {
            if (now - cacheData.timestamp > contentCacheTtl) this.lastSentContent.delete(p);
        }
        if (this.lastSentContent.size > MAX_SENT_CONTENT_CACHE) {
            const sorted = Array.from(this.lastSentContent.entries()).sort((a, b) => a[1].timestamp - b[1].timestamp);
            for (const [p] of sorted.slice(0, this.lastSentContent.size - MAX_SENT_CONTENT_CACHE)) {
                this.lastSentContent.delete(p);
            }
        }

        if (statusChanged) this.updateStatus();
    }

    async applyFileDelta(data: FileDeltaPayload) {
        await this.runLocked(data.path, async () => {
            const existingFile = this.app.vault.getAbstractFileByPath(data.path);
            if (!(existingFile instanceof TFile)) {
                throw new Error("IntegrityError: File not found for delta sync");
            }
            
            if (this.isTwoDeviceMode() && data.versionVector) {
                const localVV = this.twoDeviceState.fileVersions[data.path] || {};
                if (this.isNewerThan(localVV, data.versionVector) && !this.isNewerThan(data.versionVector, localVV)) {
                    return;
                }
            } else if (data.mtime <= existingFile.stat.mtime + this.settings.mtimeTolerance &&
                       data.mtime >= existingFile.stat.mtime - this.settings.mtimeTolerance) {
                // Within mtime tolerance — fall through to baseHash check below instead of
                // silently dropping the delta. If baseHash matches, the delta is valid and
                // should be applied; if not, IntegrityError is thrown and triggers full resend.
            }

            const localContent = await this.app.vault.read(existingFile);
            const localHash = await this.getHash(localContent);
            
            if (localHash !== data.baseHash) {
                throw new Error("IntegrityError: Base hash mismatch for delta sync");
            }
            
            const dmp = new DiffMatchPatch();
            const patches = dmp.patch_fromText(data.patches);
            const [newContent, results] = dmp.patch_apply(patches, localContent);
            
            const success = results.every((r: boolean) => r === true);
            if (!success) {
                throw new Error("IntegrityError: Patch apply failed");
            }
            
            this.ignoreNextEventForPath(data.path);
            await this.app.vault.modify(existingFile, newContent, { mtime: data.mtime });
            
            if (this.isTwoDeviceMode() && data.versionVector) {
                this.twoDeviceState.fileVersions[data.path] = data.versionVector;
                this.debouncedSaveState();
            }
            
            const newHash = await this.getHash(newContent);
            this.updateHashCache(data.path, newHash);
        });
    }

    async applyFileUpdate(data: FileUpdatePayload) {
        if (!this.isPathSyncable(data.path)) return;

        if (data.compressed && data.content instanceof ArrayBuffer) {
            data.content = decompressText(data.content);
            data.encoding = 'utf8';
        }

        let computedHash = '';
        try {
            computedHash = await this.getHash(data.content);
        } catch(e) {}

        if (data.fileHash && computedHash && data.fileHash !== computedHash) {
            throw new Error('IntegrityError: fileHash mismatch');
        }

        await this.runLocked(data.path, async () => {
            if (computedHash) {
                this.updateHashCache(data.path, computedHash);
            }

            const existingFile = this.app.vault.getAbstractFileByPath(data.path);
            if (!existingFile) {
                await this.handleNewFileCreation(data);
                if (this.isTwoDeviceMode() && data.versionVector) {
                    this.twoDeviceState.fileVersions[data.path] = data.versionVector;
                    this.debouncedSaveState();
                }
            } else if (existingFile instanceof TFile) {
                await this.handleFileModification(data, existingFile);
            } else {
                this.log(`Received file update for a path that is a folder: ${data.path}. Ignoring.`);
            }
        });
    }

    private async handleNewFileCreation(data: FileUpdatePayload) {
        this.log(`Creating new file: ${data.path}`);
        this.ignoreNextEventForPath(data.path);
        try {
            const folderPath = data.path.substring(0, data.path.lastIndexOf('/'));
            if (folderPath) {
                await this.ensureFolderExists(folderPath);
            }
            if (data.encoding === 'binary' || data.encoding === 'base64') {
                await this.app.vault.createBinary(data.path, data.content as ArrayBuffer);
            } else {
                await this.app.vault.create(data.path, data.content as string);
            }
        } catch (e) {
            if (e instanceof Error && e.message.includes("File already exists")) {
                this.log(`File ${data.path} already exists, falling back to modification.`);
                const file = this.app.vault.getAbstractFileByPath(data.path);
                if (file instanceof TFile) await this.handleFileModification(data, file);
            } else {
                console.error("File creation error:", e);
                this.showNotice(`Failed to create file: ${data.path}`, 'error');
                throw e;
            }
        }
    }

    private async ensureFolderExists(path: string) {
        const folders = path.split('/');
        let currentPath = '';
        for (const folder of folders) {
            currentPath = currentPath === '' ? folder : `${currentPath}/${folder}`;
            if (!this.app.vault.getAbstractFileByPath(currentPath)) {
                try {
                    await this.app.vault.createFolder(currentPath);
                } catch (e) { /* Ignore if created concurrently */ }
            }
        }
    }

    private async handleFileModification(data: FileUpdatePayload, existingFile: TFile) {
        try {
            const localContent = (data.encoding === 'binary' || data.encoding === 'base64')
                ? await this.app.vault.readBinary(existingFile)
                : await this.app.vault.cachedRead(existingFile);

            const contentIsSame = (data.encoding === 'binary' || data.encoding === 'base64')
                ? await this.areArrayBuffersEqual(localContent as ArrayBuffer, data.content as ArrayBuffer)
                : localContent === data.content;

            if (contentIsSame) {
                this.log(`Ignoring update (content is identical): ${data.path}`);
                if (this.isTwoDeviceMode() && data.versionVector) {
                    this.twoDeviceState.fileVersions[data.path] = this.mergeVersions(this.twoDeviceState.fileVersions[data.path] || {}, data.versionVector);
                    this.debouncedSaveState();
                }
                return;
            }

            if (this.isTwoDeviceMode() && data.versionVector) {
                const localVV = this.twoDeviceState.fileVersions[data.path] || {};
                const remoteVV = data.versionVector;
                const isRemoteNewer = this.isNewerThan(remoteVV, localVV);
                const isLocalNewer = this.isNewerThan(localVV, remoteVV);

                if (isRemoteNewer && !isLocalNewer) {
                    this.log(`Applying update (remote vector dominates): ${data.path}`);
                    this.ignoreNextEventForPath(data.path);
                    if (data.encoding === 'binary' || data.encoding === 'base64') {
                        await this.app.vault.modifyBinary(existingFile, data.content as ArrayBuffer, { mtime: data.mtime });
                    } else {
                        await this.app.vault.modify(existingFile, data.content as string, { mtime: data.mtime });
                    }
                    this.twoDeviceState.fileVersions[data.path] = remoteVV;
                    this.debouncedSaveState();
                    return;
                } else if (isLocalNewer && !isRemoteNewer) {
                    this.log(`Ignoring update (local vector dominates): ${data.path}`);
                    return;
                } else {
                    await this.resolveConflict(data, existingFile, localContent);
                    return;
                }
            }

            if (data.mtime > existingFile.stat.mtime + this.settings.mtimeTolerance) {
                this.log(`Applying update (remote is newer): ${data.path}`);
                this.ignoreNextEventForPath(data.path);
                if (data.encoding === 'binary' || data.encoding === 'base64') {
                    await this.app.vault.modifyBinary(existingFile, data.content as ArrayBuffer, { mtime: data.mtime });
                } else {
                    await this.app.vault.modify(existingFile, data.content as string, { mtime: data.mtime });
                }
                return;
            }

            if (data.mtime < existingFile.stat.mtime - this.settings.mtimeTolerance) {
                this.log(`Ignoring update (local is newer): ${data.path}`);
                return;
            }

            await this.resolveConflict(data, existingFile, localContent);
        } catch (e) {
            if (e instanceof Error && (e.message.includes("File not found") || e.message.includes("no such file"))) {
                this.log(`File ${data.path} not found during modification, falling back to creation.`);
                await this.handleNewFileCreation(data);
            } else {
                console.error(`Error modifying file ${data.path}:`, e);
                throw e;
            }
        }
    }

    private async resolveConflict(data: FileUpdatePayload, existingFile: TFile, localContent: string | ArrayBuffer) {
        this.showNotice(`Conflict detected for: ${data.path}`, 'important', 10000);
        const strategy = this.getConflictStrategy();
        this.log(`Conflict detected for: ${data.path}. Strategy: ${strategy}`);

        switch (strategy) {
            case 'role-based':
                if (this.currentRole === 'primary') {
                    this.log(`Conflict resolved by 'role-based' (Primary wins - keeping local): ${data.path}`);
                    const localVV = this.twoDeviceState.fileVersions[data.path] || {};
                    const merged = this.mergeVersions(localVV, data.versionVector || {});
                    merged[this.settings.deviceId] = (merged[this.settings.deviceId] || 0) + 1;
                    this.twoDeviceState.fileVersions[data.path] = merged;
                    this.debouncedSaveState();
                    
                    // Debounce re-send to prevent tight conflict resolution loops:
                    // if we just resolved this path, skip the immediate re-send.
                    // The file will be sent on next edit or full sync.
                    const lastResolved = this.ignoreEvents.get(`conflict:${data.path}`);
                    if (!lastResolved || Date.now() > lastResolved) {
                        this.ignoreEvents.set(`conflict:${data.path}`, Date.now() + 5000);
                        if (this.twoDevicePeerId) {
                            this.sendFileUpdate(existingFile, this.twoDevicePeerId, true);
                        }
                    } else {
                        this.log(`Skipping re-send for ${data.path} — conflict cooldown active`);
                    }
                } else {
                    this.log(`Conflict resolved by 'role-based' (Secondary yields - adopting remote): ${data.path}`);
                    this.ignoreNextEventForPath(existingFile.path);
                    if (data.encoding === 'binary' || data.encoding === 'base64') {
                        await this.app.vault.modifyBinary(existingFile, data.content as ArrayBuffer, { mtime: data.mtime });
                    } else {
                        await this.app.vault.modify(existingFile, data.content as string, { mtime: data.mtime });
                    }
                    this.twoDeviceState.fileVersions[data.path] = data.versionVector || {};
                }
                break;

            case 'last-write-wins':
                if (data.mtime > existingFile.stat.mtime) {
                    this.log(`Conflict resolved by 'last-write-wins' (remote wins): ${data.path}`);
                    this.ignoreNextEventForPath(existingFile.path);
                    if (data.encoding === 'binary' || data.encoding === 'base64') {
                        await this.app.vault.modifyBinary(existingFile, data.content as ArrayBuffer, { mtime: data.mtime });
                    } else {
                        await this.app.vault.modify(existingFile, data.content as string, { mtime: data.mtime });
                    }
                } else {
                    this.log(`Conflict resolved by 'last-write-wins' (local wins): ${data.path}`);
                }
                break;

            case 'create-conflict-file':
            default:
                this.log(`Creating conflict file for: ${data.path}`);
                await this.createConflictFile(data);
                break;
        }
    }

    async applyFileBatchBinary(data: FileBatchBinaryPayload): Promise<{ succeeded: string[], failed: string[] }> {
        const results = { succeeded: [] as string[], failed: [] as string[] };
        let buffer: ArrayBuffer;
        
        if (data.data instanceof Uint8Array) {
            buffer = data.data.buffer.slice(data.data.byteOffset, data.data.byteOffset + data.data.byteLength);
        } else if (typeof data.data === 'string') {
            buffer = base64ToArrayBuffer(data.data);
        } else {
            buffer = data.data;
        }

        let unpacked: PackedFile[];
        try {
            unpacked = unpackTLVToFiles(buffer);
        } catch (e) {
            this.log("Failed to unpack TLV binary batch", e);
            throw e;
        }

        const writePromises = unpacked.map(async (fileData) => {
            try {
                let contentStr = '';
                let contentBuf: ArrayBuffer | null = null;
                
                if (fileData.encoding === 'binary') {
                    if (fileData.isCompressed) {
                        contentStr = decompressText(fileData.content);
                    } else {
                        contentBuf = fileData.content instanceof Uint8Array ? 
                            fileData.content.buffer.slice(fileData.content.byteOffset, fileData.content.byteOffset + fileData.content.byteLength) : 
                            fileData.content;
                    }
                } else if (fileData.encoding === 'base64') {
                    // For Base64 encoded ArrayBuffers (fallback/DirectIP)
                    contentBuf = typeof fileData.content === 'string' ? base64ToArrayBuffer(fileData.content) : 
                        (fileData.content instanceof Uint8Array ? 
                            fileData.content.buffer.slice(fileData.content.byteOffset, fileData.content.byteOffset + fileData.content.byteLength) : 
                            fileData.content);
                } else {
                    contentStr = ObsidianDecentralizedPlugin.textDecoder.decode(fileData.content);
                }

                await this.runLocked(fileData.path, async () => {
                    const existingFile = this.app.vault.getAbstractFileByPath(fileData.path);
                    if (existingFile instanceof TFile) {
                        this.ignoreNextEventForPath(fileData.path);
                        if (contentBuf) {
                            await this.app.vault.modifyBinary(existingFile, contentBuf);
                        } else {
                            await this.app.vault.modify(existingFile, contentStr);
                        }
                    } else {
                        // Create parent folders if missing
                        const pathParts = fileData.path.split('/');
                        let currentPath = '';
                        for (let i = 0; i < pathParts.length - 1; i++) {
                            currentPath += (i > 0 ? '/' : '') + pathParts[i];
                            const folder = this.app.vault.getAbstractFileByPath(currentPath);
                            if (!folder) {
                                try {
                                    await this.app.vault.createFolder(currentPath);
                                } catch (e) { /* Ignore if created concurrently */ }
                            }
                        }
                        this.ignoreNextEventForPath(fileData.path);
                        if (contentBuf) {
                            await this.app.vault.createBinary(fileData.path, contentBuf);
                        } else {
                            await this.app.vault.create(fileData.path, contentStr);
                        }
                    }
                    this.syncState.filesTransferred++;
                    this.syncState.bytesTransferred += fileData.content.byteLength;
                });
                
                return fileData.path;
            } catch (e) {
                this.log(`Failed to write batched file ${fileData.path}`, e);
                throw fileData.path; // throw path on failure to track it
            }
        });

        const settled = await Promise.allSettled(writePromises);
        for (const result of settled) {
            if (result.status === 'fulfilled') {
                results.succeeded.push(result.value);
            } else {
                results.failed.push(result.reason);
            }
        }

        return results;
    }

    async createConflictFile(data: FileUpdatePayload) {
        const conflictPath = this.getConflictPath(data.path);
        this.ignoreNextEventForPath(conflictPath);
        const folderPath = conflictPath.substring(0, conflictPath.lastIndexOf('/'));
        if (folderPath) await this.ensureFolderExists(folderPath);
        if (data.encoding === 'binary' || data.encoding === 'base64') {
            await this.app.vault.createBinary(conflictPath, data.content as ArrayBuffer);
        } else {
            await this.app.vault.create(conflictPath, data.content as string);
        }
        this.conflictCenter.addConflict(data.path, conflictPath);
    }

    async applyFileDelete(data: FileDeletePayload) {
        if (!this.isPathSyncable(data.path)) return;
        await this.runLocked(data.path, async () => {
            if (this.isTwoDeviceMode() && data.versionVector) {
                const localVV = this.twoDeviceState.fileVersions[data.path] || {};
                const remoteVV = data.versionVector;
                const isRemoteNewer = this.isNewerThan(remoteVV, localVV);

                if (!isRemoteNewer) {
                    // Local edit wins (local is strictly newer, or there's a concurrent conflict).
                    // We merge the remote vector, increment local version, and push the local file back to the peer.
                    this.log(`Edit-vs-delete conflict: local edit wins for ${data.path}`);
                    const merged = this.mergeVersions(localVV, remoteVV);
                    merged[this.settings.deviceId] = (merged[this.settings.deviceId] || 0) + 1;
                    this.twoDeviceState.fileVersions[data.path] = merged;
                    this.debouncedSaveState();

                    const file = this.app.vault.getAbstractFileByPath(data.path);
                    if (file instanceof TFile && this.twoDevicePeerId) {
                        this.sendFileUpdate(file, this.twoDevicePeerId, true);
                    }
                    return;
                } else {
                    // Remote delete dominates. Update version vector to reflect the deletion.
                    this.twoDeviceState.fileVersions[data.path] = remoteVV;
                    this.debouncedSaveState();
                }
            }

            this.tombstones[data.path] = Date.now();
            this.debouncedSaveState();
            this.syncedHashes.delete(data.path);
            const existingFile = this.app.vault.getAbstractFileByPath(data.path);
            if (existingFile) {
                try {
                    this.log(`Deleting file: ${data.path}`);
                    this.ignoreNextEventForPath(data.path);
                    await this.app.vault.delete(existingFile);
                } catch (e) {
                    console.error(`Error deleting file: ${data.path}`, e);
                    this.showNotice(`Failed to delete file: ${data.path}`, 'error');
                }
            }
        });
    }

    async applyFileRename(data: FileRenamePayload) { 
        if (!this.isPathSyncable(data.oldPath) && !this.isPathSyncable(data.newPath)) return; 
        const [firstLock, secondLock] = [data.oldPath, data.newPath].sort();
        await this.runLocked(firstLock, async () => {
            await this.runLocked(secondLock, async () => {
                const fileToRename = this.app.vault.getAbstractFileByPath(data.oldPath); 
                if (fileToRename instanceof TFile) { 
                    try { 
                        this.log(`Renaming file: ${data.oldPath} -> ${data.newPath}`); 
                        this.ignoreNextEventForPath(data.newPath); 
                        const cached = this.syncedHashes.get(data.oldPath); 
                        if (cached) { 
                            this.syncedHashes.set(data.newPath, cached); 
                            this.syncedHashes.delete(data.oldPath); 
                        } 
                        if (data.versionVector) {
                            this.twoDeviceState.fileVersions[data.newPath] = data.versionVector;
                            delete this.twoDeviceState.fileVersions[data.oldPath];
                        }
                        this.debouncedSaveState(); 
                        await this.app.vault.rename(fileToRename, data.newPath); 
                    } catch (e) { 
                        console.error(`Error renaming file: ${data.oldPath} -> ${data.newPath}`, e); 
                        this.showNotice(`Failed to rename file: ${data.oldPath}`, 'error'); 
                    } 
                } else {
                    const newFile = this.app.vault.getAbstractFileByPath(data.newPath);
                    if (newFile instanceof TFile && data.versionVector) {
                        this.twoDeviceState.fileVersions[data.newPath] = data.versionVector;
                        delete this.twoDeviceState.fileVersions[data.oldPath];
                        this.debouncedSaveState();
                    }
                }
            });
        });
    }

    async applyFolderCreate(data: FolderCreatePayload) { 
        if (!this.isPathSyncable(data.path)) return; 
        await this.runLocked(data.path, async () => {
            if (this.app.vault.getAbstractFileByPath(data.path)) return; 
            this.log(`Creating folder: ${data.path}`); 
            this.ignoreNextEventForPath(data.path); 
            try { 
                await this.app.vault.createFolder(data.path); 
            } catch (e) { 
                console.error(`Failed to create folder ${data.path}`, e); 
            } 
        });
    }

    async applyFolderDelete(data: FolderDeletePayload) { 
        if (!this.isPathSyncable(data.path)) return; 
        await this.runLocked(data.path, async () => {
            const folder = this.app.vault.getAbstractFileByPath(data.path); 
            if (folder instanceof TFolder) { 
                this.log(`Deleting folder: ${data.path}`); 
                this.ignoreNextEventForPath(data.path, 5000); 
                try { 
                    await this.app.vault.delete(folder, true); 
                } catch (e) { 
                    console.error(`Failed to delete folder ${data.path}`, e); 
                } 
            } 
        });
    }

    async applyFolderRename(data: FolderRenamePayload) { 
        if (!this.isPathSyncable(data.oldPath) && !this.isPathSyncable(data.newPath)) return; 
        const [firstLock, secondLock] = [data.oldPath, data.newPath].sort();
        await this.runLocked(firstLock, async () => {
            await this.runLocked(secondLock, async () => {
                const folder = this.app.vault.getAbstractFileByPath(data.oldPath); 
                if (folder instanceof TFolder) { 
                    this.log(`Renaming folder: ${data.oldPath} -> ${data.newPath}`); 
                    this.ignoreNextEventForPath(data.oldPath); 
                    this.ignoreNextEventForPath(data.newPath); 
                    try { 
                        await this.app.vault.rename(folder, data.newPath); 
                    } catch (e) { 
                        console.error(`Failed to rename folder ${data.oldPath}`, e); 
                    } 
                } 
            });
        });
    }

    async requestFullSyncFromPeer(peerId: string) { 
        if (this.syncState.isSyncing) { this.showNotice("A sync is already in progress.", 'info'); return; } 
        const conn = this.connections.get(peerId); 
        if (!conn) { this.showNotice("Peer not found.", 'error'); return; } 
        this.showNotice(`Starting full sync with ${this.clusterPeers.get(peerId)?.friendlyName}...`, 'info'); 
        this.syncState.isSyncing = true; 
        this.syncState.peerId = peerId;
        this.syncState.filesTotal = 0;
        this.syncState.filesTransferred = 0;
        this.syncState.bytesTotal = 0;
        this.syncState.bytesTransferred = 0;
        this.syncState.syncStartTime = Date.now();
        this.syncState.currentFile = null;
        this.syncState.currentFileSize = null;
        this.syncState.inFlightPulls = new Set();
        this.syncState.activePullBatches = new Set();
        this.syncState.adaptiveConfig = {
            maxActiveBatches: 1,
            filesPerBatch: 50,
            maxBytesPerBatch: 50 * 1024 * 1024
        };
        this.syncState.batchStartTimes = new Map();
        this.currentSyncIsTwoDeviceMode = this.isTwoDeviceMode();
        this.localSyncComplete.set(peerId, false);
        this.peerSyncComplete.set(peerId, false);
        this.transitionToPhase(SyncPhase.REQUESTING);
        this.startSyncKeepAlive();
        this.resetIdleTimeout();
        
        try {
            const localManifest = await this.buildVaultManifest(); 
            this.log(`Sending sync request with ${localManifest.length} items.`); 
            await this.sendSyncMessage(peerId, { type: 'request-full-sync', manifest: localManifest }); 
        } catch (e) {
            this.abortSync(e instanceof SyncError ? e : new SyncError(SyncErrorCategory.PROTOCOL_ERROR, String(e), false, "Check network connection."));
        }
    }
    
    // --- Merkle Handlers ---
    async handleMerkleRoot(data: MerkleRootPayload, conn: DataConnection) {
        // Merkle reconciliation runs as BACKGROUND convergence over the normal queue —
        // it must not claim the full-sync state machine: the traversal has no completion
        // signal, so claiming isSyncing here left the plugin stuck in "Requesting Sync..."
        // until the phase timeout aborted with an error.
        if (this.syncState.isSyncing) return;
        const tree = await this.buildMerkleTree();
        if (tree.hash === data.rootHash) {
            this.log("Merkle roots match — vaults already in sync.");
        } else {
            this.log(`Merkle roots differ. Initiating tree traversal.`);
            this.sendData(conn.peer, { type: 'merkle-node-request', path: '' });
        }
    }
    
    async handleMerkleNodeRequest(data: MerkleNodeRequestPayload, conn: DataConnection) {
        this.resetIdleTimeout();
        const tree = this.twoDeviceState.merkleTreeRoot;
        if (!tree) return;
        
        let targetNode = tree;
        if (data.path !== '') {
            const parts = data.path.split('/');
            for (const p of parts) {
                if (targetNode.children && targetNode.children[p]) {
                    targetNode = targetNode.children[p];
                } else {
                    return; // Node not found
                }
            }
        }
        
        const childHashes: Record<string, string> = {};
        if (targetNode.children) {
            for (const [key, node] of Object.entries(targetNode.children)) {
                childHashes[key] = node.hash;
            }
        }
        this.sendData(conn.peer, { type: 'merkle-node-response', path: data.path, children: childHashes });
    }
    
    async handleMerkleNodeResponse(data: MerkleNodeResponsePayload, conn: DataConnection) {
        this.resetIdleTimeout();
        const tree = this.twoDeviceState.merkleTreeRoot;
        if (!tree) return;
        
        let targetNode = tree;
        if (data.path !== '') {
            const parts = data.path.split('/');
            for (const p of parts) {
                if (targetNode.children && targetNode.children[p]) targetNode = targetNode.children[p];
            }
        }
        
        const myChildren = targetNode.children || {};
        const remoteChildren = data.children;
        
        const allKeys = new Set([...Object.keys(myChildren), ...Object.keys(remoteChildren)]);
        
        for (const key of allKeys) {
            const myHash = myChildren[key]?.hash;
            const remoteHash = remoteChildren[key];
            const fullPath = data.path ? `${data.path}/${key}` : key;
            
            if (myHash !== remoteHash) {
                const file = this.app.vault.getAbstractFileByPath(fullPath);
                if (file instanceof TFolder || (!file && !fullPath.includes('.'))) {
                    this.sendData(conn.peer, { type: 'merkle-node-request', path: fullPath });
                } else {
                    if (!myHash && remoteHash) {
                        this.sendData(conn.peer, { type: 'request-file', path: fullPath });
                    } else if (file instanceof TFile) {
                        // Exchange BOTH directions: push ours and pull theirs. Each side's
                        // conflict resolution (version vectors / role-based) then picks the
                        // same winner deterministically. Pushing only our copy left the peer
                        // stale whenever its version-vector dominated ours.
                        this.sendFileUpdate(file, conn.peer);
                        if (remoteHash) {
                            this.sendData(conn.peer, { type: 'request-file', path: fullPath });
                        }
                    }
                }
            }
        }
    }

    async handleFullSyncRequest(data: FullSyncRequestPayload, conn: DataConnection) { 
        if (this.syncState.isSyncing) { 
            this.log(`Received a sync request from ${conn.peer}, but a sync is already in progress in phase ${this.syncState.currentPhase}. Ignoring.`); 
            return; 
        } 
        try {
            if (!data.manifest) throw new SyncError(SyncErrorCategory.PROTOCOL_ERROR, "Received invalid sync request (missing manifest).", false, "Update plugin on both devices.");
            this.showNotice(`Peer ${this.clusterPeers.get(conn.peer)?.friendlyName} requested a full sync. Comparing vaults...`, 'info'); 
            this.syncState.isSyncing = true; 
            this.syncState.peerId = conn.peer;
            this.syncState.filesTotal = 0;
            this.syncState.filesTransferred = 0;
            this.syncState.bytesTotal = 0;
            this.syncState.bytesTransferred = 0;
            this.syncState.syncStartTime = Date.now();
            this.syncState.currentFile = null;
            this.syncState.currentFileSize = null;
            this.syncState.inFlightPulls = new Set();
            this.syncState.activePullBatches = new Set();
            this.syncState.adaptiveConfig = {
                maxActiveBatches: 1,
                filesPerBatch: 50,
                maxBytesPerBatch: 50 * 1024 * 1024
            };
            this.syncState.batchStartTimes = new Map();
            this.currentSyncIsTwoDeviceMode = this.isTwoDeviceMode();
            this.localSyncComplete.set(conn.peer, false);
            this.peerSyncComplete.set(conn.peer, false);
            this.transitionToPhase(SyncPhase.PLANNING);
            this.startSyncKeepAlive();
            this.resetIdleTimeout();
        
            const remoteManifest = data.manifest; 
            const localManifest = await this.buildVaultManifest(); 
            const remoteIndex = new Map(remoteManifest.map(item => [item.path, item])); 
            const localIndex = new Map(localManifest.map(item => [item.path, item])); 
            
            const filesReceiverWillSend: string[] = []; 
            const filesInitiatorMustSend: string[] = []; 
            const filesReceiverMustDelete: string[] = [];
            const filesInitiatorMustDelete: string[] = [];
            const fileSizes: Record<string, number> = {};
            
            const allPaths = new Set([...localIndex.keys(), ...remoteIndex.keys()]);
            
            let peerPotentiallyStale = false;
            const retentionMs = (this.settings.tombstoneRetentionDays || 30) * 24 * 60 * 60 * 1000;
            const now = Date.now();
            
            for (const path of allPaths) {
                const localItem = localIndex.get(path);
                const remoteItem = remoteIndex.get(path);
                
                if (localItem && !remoteItem) {
                    if (localItem.type === 'file') {
                        filesReceiverWillSend.push(path);
                        fileSizes[path] = localItem.size;
                    }
                } else if (!localItem && remoteItem) {
                    if (remoteItem.type === 'file') {
                        if (now - remoteItem.mtime > retentionMs) peerPotentiallyStale = true;
                        filesInitiatorMustSend.push(path);
                        this.peerFileSizes[path] = remoteItem.size;
                    }
                } else if (localItem && remoteItem) {
                    if (localItem.type === 'file' && remoteItem.type === 'file') {
                        let conflictResolved = false;
                        if (this.currentSyncIsTwoDeviceMode && localItem.versionVector && remoteItem.versionVector) {
                            const isLocalNewer = this.isNewerThan(localItem.versionVector, remoteItem.versionVector);
                            const isRemoteNewer = this.isNewerThan(remoteItem.versionVector, localItem.versionVector);
                            if (isLocalNewer && !isRemoteNewer) {
                                filesReceiverWillSend.push(path);
                                fileSizes[path] = localItem.size;
                                conflictResolved = true;
                            }
                            else if (isRemoteNewer && !isLocalNewer) {
                                filesInitiatorMustSend.push(path);
                                this.peerFileSizes[path] = remoteItem.size;
                                conflictResolved = true;
                            }
                            else if (!isLocalNewer && !isRemoteNewer) {
                                if (localItem.hash && remoteItem.hash && localItem.hash === remoteItem.hash) {
                                    conflictResolved = true;
                                } else {
                                    const role = this.getMyRole(conn.peer);
                                    if (role === 'primary') {
                                        filesReceiverWillSend.push(path);
                                        fileSizes[path] = localItem.size;
                                    } else {
                                        filesInitiatorMustSend.push(path);
                                        this.peerFileSizes[path] = remoteItem.size;
                                    }
                                    conflictResolved = true;
                                }
                            }
                        } 
                        
                        if (!conflictResolved) {
                            if (localItem.size !== remoteItem.size) {
                                if (localItem.mtime > remoteItem.mtime) { filesReceiverWillSend.push(path); fileSizes[path] = localItem.size; }
                                else { filesInitiatorMustSend.push(path); this.peerFileSizes[path] = remoteItem.size; }
                            } else if (Math.abs(localItem.mtime - remoteItem.mtime) > this.settings.mtimeTolerance) {
                                let lHash = localItem.hash;
                                if (!lHash) {
                                    const file = this.app.vault.getAbstractFileByPath(path);
                                    if (file instanceof TFile) {
                                        const content = this.isBinary(file.extension) ? await this.app.vault.readBinary(file) : await this.app.vault.read(file);
                                        lHash = await this.getHash(content);
                                        this.updateHashCache(path, lHash);
                                    }
                                }
                                if (remoteItem.hash && lHash === remoteItem.hash) {
                                    // Match
                                } else {
                                    if (localItem.mtime > remoteItem.mtime) { filesReceiverWillSend.push(path); fileSizes[path] = localItem.size; }
                                    else { filesInitiatorMustSend.push(path); this.peerFileSizes[path] = remoteItem.size; }
                                }
                            }
                        }
                    } else if (localItem.type === 'deleted' && remoteItem.type === 'file') {
                        // Tombstone mtime = deletion time; file mtime = last modification time.
                        // Comparing directly is intentional: if the file was modified AFTER it
                        // was deleted on the other side, the newer modification takes precedence.
                        if (localItem.mtime > remoteItem.mtime) filesInitiatorMustDelete.push(path);
                        else { filesInitiatorMustSend.push(path); this.peerFileSizes[path] = remoteItem.size; }
                    } else if (localItem.type === 'file' && remoteItem.type === 'deleted') {
                        if (remoteItem.mtime > localItem.mtime) filesReceiverMustDelete.push(path);
                        else { filesReceiverWillSend.push(path); fileSizes[path] = localItem.size; }
                    }
                }
            }
            
            if (peerPotentiallyStale) {
                this.showNotice("Warning: Peer has files older than your tombstone retention period. This might resurrect deleted files.", 'warning', 15000);
            }
            
            this.log(`Sync plan: They pull ${filesReceiverWillSend.length}, I pull ${filesInitiatorMustSend.length}, They delete ${filesInitiatorMustDelete.length}, I delete ${filesReceiverMustDelete.length}`); 
            this.syncState.filesTotal = filesReceiverWillSend.length + filesInitiatorMustSend.length;
            if (filesReceiverWillSend.length === 0 && filesInitiatorMustSend.length === 0 && filesReceiverMustDelete.length === 0 && filesInitiatorMustDelete.length === 0) {
                this.log("Vaults are completely identical. No sync needed.");
            }
            
            await this.sendSyncMessage(conn.peer, { type: 'sync-plan', filesReceiverWillSend, filesInitiatorMustSend, filesReceiverMustDelete, filesInitiatorMustDelete, fileSizes }); ; 
            
            for (const path of filesReceiverMustDelete) {
                await this.runLocked(path, async () => {
                    const file = this.app.vault.getAbstractFileByPath(path);
                    if (file) {
                        try {
                            this.ignoreNextEventForPath(path);
                            await this.app.vault.delete(file);
                            this.syncedHashes.delete(path);
                            if (this.isTwoDeviceMode()) {
                                this.incrementVersion(path);
                            }
                            this.tombstones[path] = Date.now();
                            this.debouncedSaveState();
                        } catch (e) {
                            this.log(`Failed to delete file ${path}:`, e);
                        }
                    }
                });
            }
            
            this.syncState.allowedPulls = new Set(filesReceiverWillSend);
            this.syncState.pendingPulls = new Set(filesInitiatorMustSend);
            this.requestNextBatch(conn.peer);
            
        } catch (e) {
            this.abortSync(e instanceof SyncError ? e : new SyncError(SyncErrorCategory.PROTOCOL_ERROR, String(e), false, "Check logs for error details."));
        }
    }
    
    async handleSyncPlan(data: SyncPlanPayload, conn: DataConnection) {
        if (!this.syncState.isSyncing || this.syncState.peerId !== conn.peer) return;
        if (this.syncState.currentPhase !== SyncPhase.PLANNING && this.syncState.currentPhase !== SyncPhase.REQUESTING) {
            this.log(`Received sync plan but current phase is ${this.syncState.currentPhase}. Ignoring.`);
            return;
        }
        this.transitionToPhase(SyncPhase.PLANNING);
        this.resetIdleTimeout();
        try {
            if (!data.filesReceiverWillSend || !data.filesInitiatorMustSend) throw new SyncError(SyncErrorCategory.PROTOCOL_ERROR, "Invalid sync plan received.", false, "Update plugin.");
            
            this.showNotice("Received sync plan. Exchanging files...", 'verbose'); 
            this.log(`Sync plan: I must pull ${data.filesReceiverWillSend.length}, They pull ${data.filesInitiatorMustSend.length}`); 
            this.syncState.filesTotal = data.filesReceiverWillSend.length + data.filesInitiatorMustSend.length;
            
            for (const path of data.filesInitiatorMustDelete || []) {
                await this.runLocked(path, async () => {
                    const file = this.app.vault.getAbstractFileByPath(path);
                    if (file) {
                        try {
                            this.ignoreNextEventForPath(path);
                            await this.app.vault.delete(file);
                            this.syncedHashes.delete(path);
                            if (this.isTwoDeviceMode()) {
                                this.incrementVersion(path);
                            }
                            this.tombstones[path] = Date.now();
                            this.debouncedSaveState();
                        } catch (e) {
                            this.log(`Failed to delete file ${path}:`, e);
                        }
                    }
                });
            }
            
            const receiverWillSendSet = new Set(data.filesReceiverWillSend);
            for (const [path, size] of Object.entries(data.fileSizes)) {
                this.peerFileSizes[path] = size;
                if (receiverWillSendSet.has(path)) this.syncState.bytesTotal += size;
            }
            
            this.syncState.allowedPulls = new Set(data.filesInitiatorMustSend);
            this.syncState.pendingPulls = new Set(data.filesReceiverWillSend);
            
            this.requestNextBatch(conn.peer);
        } catch (e) {
            this.log('Error processing sync plan:', e);
            this.abortSync(e instanceof SyncError ? e : new SyncError(SyncErrorCategory.PROTOCOL_ERROR, String(e), false, "Check logs for details."));
        }
    }
    
    private recordBatchTaskCompletion(batchId: string | undefined, paths: string[] | undefined, success: boolean, reauthorizeOnFailure = true) {
        if (!batchId || !paths || paths.length === 0) return;
        const batch = this.syncState.activeBatches.get(batchId);
        if (!batch) return;

        // Count every path covered by the completed task. A 'send-file-batch' task
        // covers many paths but completes as ONE queue item — counting it as 1 while
        // totalCount counts paths would leave sentCount < totalCount forever, so the
        // batch-complete message would never be sent and the sync would deadlock.
        batch.sentCount += paths.length;
        if (success) {
            batch.succeededPaths.push(...paths);
        } else {
            batch.failedPaths.push(...paths);
            // Re-authorize failed paths so the peer's retry request isn't rejected as "unauthorized"
            if (reauthorizeOnFailure) {
                for (const p of paths) this.syncState.allowedPulls.add(p);
            }
        }

        if (batch.sentCount >= batch.totalCount) {
            this.sendSyncMessage(batch.peerId, { type: 'batch-complete', batchId: batch.batchId, receivedPaths: batch.succeededPaths, failedPaths: batch.failedPaths })
                .catch(e => this.abortSync(e));
            this.syncState.activeBatches.delete(batchId);
            this.checkFullSyncCompletion(batch.peerId);
        }
    }

    async handleRequestBatch(data: RequestBatchPayload, conn: DataConnection) {
        if (!this.syncState.isSyncing || this.syncState.peerId !== conn.peer) return;
        if (this.syncState.currentPhase !== SyncPhase.TRANSFERRING && this.syncState.currentPhase !== SyncPhase.PLANNING) return;
        this.transitionToPhase(SyncPhase.TRANSFERRING); // Reset timeout
        this.resetIdleTimeout();
        
        try {
            if (!data.paths || !data.batchId) throw new SyncError(SyncErrorCategory.PROTOCOL_ERROR, "Invalid batch request.", false, "Update plugin.");
            
            const allowed = this.syncState.allowedPulls;
            const batchId = data.batchId;
            const batchState: BatchState = {
                peerId: conn.peer,
                batchId,
                totalCount: data.paths.length,
                sentCount: 0,
                succeededPaths: [],
                failedPaths: []
            };
            this.syncState.activeBatches.set(batchId, batchState);
            
            let currentBatchPaths: string[] = [];
            let currentBatchSize = 0;
            const MAX_BATCH_BYTES = 60 * 1024; // 60KB safe limit for WebRTC

            for (const path of data.paths) {
                if (allowed.has(path)) {
                    allowed.delete(path);
                    const file = this.app.vault.getAbstractFileByPath(path);
                    if (file instanceof TFile) {
                        const estimatedOverhead = 16 + path.length * 3; // Approx 3 bytes per char for UTF8 safety
                        const estimatedSize = file.stat.size + estimatedOverhead;

                        if (estimatedSize >= MAX_BATCH_BYTES) {
                            // Too large for binary batch, send normally (will chunk if needed)
                            this.addToQueueTask(conn.peer, { taskType: 'send-file', path, mtime: file.stat.mtime, forceFull: true, batchId });
                        } else {
                            if (currentBatchSize + estimatedSize >= MAX_BATCH_BYTES) {
                                // Flush current batch
                                this.addToQueueTask(conn.peer, { taskType: 'send-file-batch', paths: currentBatchPaths, batchId });
                                currentBatchPaths = [];
                                currentBatchSize = 0;
                            }
                            currentBatchPaths.push(path);
                            currentBatchSize += estimatedSize;
                        }
                    } else if (file instanceof TFolder) {
                        this.addToQueueTask(conn.peer, { taskType: 'send-folder-create', path, batchId });
                    } else {
                        this.log(`Failed to read ${path} for batch, marking failed.`);
                        this.recordBatchTaskCompletion(batchId, [path], false);
                    }
                } else {
                    this.log(`Peer requested unauthorized path ${path}. Marking failed.`);
                    this.recordBatchTaskCompletion(batchId, [path], false, false);
                }
            }

            if (currentBatchPaths.length > 0) {
                this.addToQueueTask(conn.peer, { taskType: 'send-file-batch', paths: currentBatchPaths, batchId });
            }
            
            if (data.paths.length === 0) {
                this.sendSyncMessage(conn.peer, { type: 'batch-complete', batchId, receivedPaths: [], failedPaths: [] }).catch(e => this.abortSync(e));
                this.syncState.activeBatches.delete(batchId);
                this.checkFullSyncCompletion(conn.peer);
            }
        } catch (e) {
            this.abortSync(e instanceof SyncError ? e : new SyncError(SyncErrorCategory.PROTOCOL_ERROR, String(e), false, "Check logs."));
        }
    }
    
    handleBatchComplete(data: BatchCompletePayload, conn: DataConnection) {
        if (!this.syncState.isSyncing || this.syncState.peerId !== conn.peer) return;
        if (this.syncState.currentPhase !== SyncPhase.TRANSFERRING) return;
        // Only accept completions for pull batches WE initiated; anything else is a
        // stray/echoed batchId and processing it would corrupt pull-tracking state.
        if (!this.syncState.activePullBatches?.has(data.batchId)) {
            this.log(`Ignoring batch-complete for unknown batch ${data.batchId}`);
            return;
        }
        this.transitionToPhase(SyncPhase.TRANSFERRING); // Reset timeout
        this.resetIdleTimeout();
        
        try {
            if (!data.receivedPaths || !data.failedPaths) throw new SyncError(SyncErrorCategory.PROTOCOL_ERROR, "Invalid batch complete payload.", false, "Update plugin.");
            const pending = this.syncState.pendingPulls;
            if (this.syncState.activePullBatches) this.syncState.activePullBatches.delete(data.batchId);
            
            const startTime = this.syncState.batchStartTimes?.get(data.batchId);
            this.syncState.batchStartTimes?.delete(data.batchId);
            const durationSec = startTime ? (Date.now() - startTime) / 1000 : 0;
            let batchBytes = 0;
            
            for (const path of data.receivedPaths) {
                pending.delete(path);
                if (this.syncState.inFlightPulls) this.syncState.inFlightPulls.delete(path);
                this.syncState.filesTransferred++;
                const size = this.peerFileSizes[path] || 0;
                this.syncState.bytesTransferred += size;
                batchBytes += size;
                this.pullRetries.delete(path);
            }
            
            for (const path of data.failedPaths) {
                if (this.syncState.inFlightPulls) this.syncState.inFlightPulls.delete(path);
                const retries = this.pullRetries.get(path) || 0;
                if (retries < 3) {
                    this.pullRetries.set(path, retries + 1);
                } else {
                    pending.delete(path);
                    this.pullRetries.delete(path);
                    this.log(`Failed to pull ${path} after 3 attempts. Giving up on this file.`);
                }
            }
            
            if (data.failedPaths.length > 0) {
                this.syncState.adaptiveConfig.maxActiveBatches = Math.max(1, Math.floor(this.syncState.adaptiveConfig.maxActiveBatches / 2));
                this.syncState.adaptiveConfig.filesPerBatch = Math.max(10, Math.floor(this.syncState.adaptiveConfig.filesPerBatch / 2));
                this.log(`AdaptiveSync: Network issues detected (${data.failedPaths.length} failed). Decreasing limits to ${this.syncState.adaptiveConfig.maxActiveBatches} batches, ${this.syncState.adaptiveConfig.filesPerBatch} files/batch.`);
            } else if (durationSec > 0 && data.receivedPaths.length > 0) {
                const throughput = batchBytes / durationSec;
                if (throughput > 100 * 1024 || durationSec < 0.5) {
                    this.syncState.adaptiveConfig.maxActiveBatches = Math.min(5, this.syncState.adaptiveConfig.maxActiveBatches + 1);
                    this.syncState.adaptiveConfig.filesPerBatch = Math.min(500, this.syncState.adaptiveConfig.filesPerBatch + 50);
                    this.log(`AdaptiveSync: Good transfer (${(throughput / 1024 / 1024).toFixed(2)} MB/s). Increasing limits to ${this.syncState.adaptiveConfig.maxActiveBatches} batches, ${this.syncState.adaptiveConfig.filesPerBatch} files/batch.`);
                }
            }
            
            this.requestNextBatch(conn.peer);
        } catch (e) {
            this.abortSync(e instanceof SyncError ? e : new SyncError(SyncErrorCategory.PROTOCOL_ERROR, String(e), false, "Check logs."));
        }
    }
    
    requestNextBatch(peerId: string) {
        if (this.syncState.currentPhase !== SyncPhase.TRANSFERRING && this.syncState.currentPhase !== SyncPhase.PLANNING) return;
        const pending = this.syncState.pendingPulls;
        if (!pending || pending.size === 0) {
            this.checkFullSyncCompletion(peerId);
            return;
        }
        
        if (!this.syncState.activePullBatches) this.syncState.activePullBatches = new Set();
        if (!this.syncState.inFlightPulls) this.syncState.inFlightPulls = new Set();
        
        while (this.syncState.activePullBatches.size < this.syncState.adaptiveConfig.maxActiveBatches) {
            // Avoid Array.from().filter() intermediate allocations — iterate the Set directly
            const inFlight = this.syncState.inFlightPulls;
            const availablePending: string[] = [];
            for (const p of pending) {
                if (!inFlight.has(p)) availablePending.push(p);
            }
            if (availablePending.length === 0) {
                if (this.syncState.activePullBatches.size === 0) {
                    this.checkFullSyncCompletion(peerId);
                }
                break;
            }

            this.transitionToPhase(SyncPhase.TRANSFERRING);        
            const paths: string[] = [];
            let totalSize = 0;
            
            const sortedPending = availablePending.sort((a, b) => (this.peerFileSizes[a] || 0) - (this.peerFileSizes[b] || 0));
            
            for (const path of sortedPending) {
                const size = this.peerFileSizes[path] || 0;
                if (paths.length >= this.syncState.adaptiveConfig.filesPerBatch || (totalSize + size > this.syncState.adaptiveConfig.maxBytesPerBatch && paths.length > 0)) {
                    break;
                }
                paths.push(path);
                totalSize += size;
                this.syncState.inFlightPulls.add(path);
            }
            
            this.log(`Requesting batch of ${paths.length} files (${formatBytes(totalSize)}). Active batches: ${this.syncState.activePullBatches.size + 1}/${this.syncState.adaptiveConfig.maxActiveBatches}`);
            const batchId = this.generateTransferId('batch');
            this.syncState.activePullBatches.add(batchId);
            this.syncState.batchStartTimes?.set(batchId, Date.now());
            this.sendSyncMessage(peerId, { type: 'request-batch', paths, batchId }).catch(e => {
                this.syncState.activePullBatches?.delete(batchId);
                this.syncState.batchStartTimes?.delete(batchId);
                for (const p of paths) this.syncState.inFlightPulls?.delete(p);
                this.abortSync(e);
            });
            this.resetIdleTimeout();
        }
    }
    
    checkFullSyncCompletion(peerId: string) {
        const pending = this.syncState.pendingPulls;
        const allowed = this.syncState.allowedPulls;
        const activeBatches = this.syncState.activeBatches;
        
        // Only declare complete when no pulls are pending, no pulls are allowed, AND
        // no batches are still in-flight (being sent by the peer).
        if ((!pending || pending.size === 0) && (!allowed || allowed.size === 0) && (!activeBatches || activeBatches.size === 0)) {
            if (this.syncState.currentPhase !== SyncPhase.COMPLETING) {
                this.transitionToPhase(SyncPhase.COMPLETING);
            }
            if (!this.localSyncComplete.get(peerId)) {
                this.localSyncComplete.set(peerId, true);
                this.sendSyncMessage(peerId, { type: 'full-sync-complete' }).catch(e => this.abortSync(e));
            }       
        }
        
        if (this.localSyncComplete.get(peerId) && this.peerSyncComplete.get(peerId) && (!activeBatches || activeBatches.size === 0)) {
            this.transitionToPhase(SyncPhase.COMPLETING);
            this.handleFullSyncComplete();
        }
    }

    handleFullSyncComplete() { 
        if (!this.syncState.isSyncing) return; 
        if (this.syncIdleTimeout) { clearTimeout(this.syncIdleTimeout); this.syncIdleTimeout = null; } 
        if (this.syncKeepAliveInterval) { clearInterval(this.syncKeepAliveInterval); this.syncKeepAliveInterval = null; }
        if (this.syncState.phaseTimeoutHandle) { clearTimeout(this.syncState.phaseTimeoutHandle); this.syncState.phaseTimeoutHandle = null; }
        this.syncState.isSyncing = false; 
        this.syncState.currentPhase = SyncPhase.IDLE;
        this.currentSyncIsTwoDeviceMode = null;
        this.syncState.pendingPulls.clear();
        this.syncState.inFlightPulls?.clear();
        this.syncState.allowedPulls.clear();
        this.pullRetries.clear();
        this.syncState.activeBatches.clear();
        this.syncState.activePullBatches?.clear();
        this.localSyncComplete.clear();
        this.peerSyncComplete.clear();
        this.syncState.peerId = null;
        this.peerFileSizes = {};
        this.processQueue(); 
        this.updateStatus(); 
        this.showNotice(`Sync complete. Transferred ${this.syncState.filesTransferred} files.`, 'important'); 
    }

    handleRequestFile(data: RequestFilePayload, conn: DataConnection) {
        const file = this.app.vault.getAbstractFileByPath(data.path);
        if (file instanceof TFile) {
            this.sendFileUpdate(file, conn.peer);
        }
    }
    
    private async buildVaultManifest(): Promise<VaultManifest> { 
        const manifest: VaultManifest = []; 
        const allFiles = this.app.vault.getAllLoadedFiles(); 

        let count = 0;
        for (const file of allFiles) { 
            if (this.isPathSyncable(file.path)) { 
                if (file instanceof TFolder) { 
                    if (file.path !== '/') manifest.push({ type: 'folder', path: file.path }); 
                } else if (file instanceof TFile) { 
                    let hash = this.syncedHashes.get(file.path)?.hash;
                    let vv = (this.currentSyncIsTwoDeviceMode ?? this.isTwoDeviceMode()) ? this.twoDeviceState.fileVersions[file.path] : undefined;
                    manifest.push({ type: 'file', path: file.path, mtime: file.stat.mtime, size: file.stat.size, hash, versionVector: vv }); 
                    
                    count++;
                    if (count % 50 === 0) {
                        this.updateStatus({ text: `Building manifest (${count}/${allFiles.length})...`, icon: 'loader', spin: true, state: 'loading' });
                        await new Promise(r => setTimeout(r, 0));
                    }
                } 
            } 
        }
        for (const [path, timestamp] of Object.entries(this.tombstones)) {
            manifest.push({ type: 'deleted', path, mtime: timestamp, size: 0 });
        }
        return manifest; 
    }

    private isPathSyncable(path: string): boolean {
        // Use cached arrays to avoid re-parsing on every vault event (invalidated on settings change)
        if (this._cachedExcludedFolders === null) {
            this._cachedExcludedFolders = this.settings.excludedFolders.split('\n').map(p => p.trim()).filter(Boolean);
        }
        if (this._cachedExcludedFolders.length > 0 && this._cachedExcludedFolders.some(p => path.startsWith(p))) { return false; }

        if (path.startsWith('.obsidian/')) {
            if (this.settings.syncMode === 'auto') {
                const safePaths = ['.obsidian/snippets/', '.obsidian/themes/', '.obsidian/appearance.json'];
                return safePaths.some(safe => path.startsWith(safe));
            } else {
                return this.settings.syncObsidianConfig;
            }
        }

        if (this.settings.syncMode === 'manual' || this.settings.syncMode === 'advanced') {
            if (this._cachedIncludedFolders === null) {
                this._cachedIncludedFolders = this.settings.includedFolders.split('\n').map(p => p.trim()).filter(Boolean);
            }
            if (this._cachedIncludedFolders.length > 0 && !this._cachedIncludedFolders.some(p => path.startsWith(p))) { return false; }
        }
        return true;
    }
    public isBinary(extension: string): boolean { const textExtensions = ['md', 'txt', 'json', 'css', 'js', 'html', 'xml', 'csv', 'yaml', 'toml']; return !textExtensions.includes((extension || '').toLowerCase()); }
    private async areArrayBuffersEqual(buf1: ArrayBuffer, buf2: ArrayBuffer): Promise<boolean> { 
        if (buf1.byteLength !== buf2.byteLength) return false; 
        if (buf1.byteLength < 50 * 1024) {
            const view1 = new Uint8Array(buf1); 
            const view2 = new Uint8Array(buf2); 
            for (let i = 0; i < buf1.byteLength; i++) { 
                if (view1[i] !== view2[i]) return false; 
            } 
            return true; 
        }
        const hash1 = await this.getHash(buf1);
        const hash2 = await this.getHash(buf2);
        return hash1 === hash2;
    }
    private shouldIgnoreEvent(path: string): boolean { const ignoreUntil = this.ignoreEvents.get(path); if (ignoreUntil && Date.now() < ignoreUntil) { return true; } this.ignoreEvents.delete(path); return false; }
    public ignoreNextEventForPath(path: string, durationMs = 2000) { this.ignoreEvents.set(path, Date.now() + durationMs); }
    getConflictPath(originalPath: string): string {
        const date = new Date().toISOString().split('T')[0];
        const lastDot = originalPath.lastIndexOf('.');
        const lastSlash = originalPath.lastIndexOf('/');
        if (lastDot > lastSlash) {
            // Has a file extension
            const base = originalPath.substring(0, lastDot);
            const extension = originalPath.substring(lastDot + 1);
            return `${base} (conflict on ${date}).${extension}`;
        }
        // No extension (e.g. README, Makefile)
        return `${originalPath} (conflict on ${date})`;
    }
    getLocalIp(): string | null { if (Platform.isMobile) return null; try { const os = require('os'); const interfaces = os.networkInterfaces(); for (const name in interfaces) { for (const net of interfaces[name]!) { if (net.family === 'IPv4' && !net.internal) return net.address; } } } catch (e) { console.warn("Could not get local IP address.", e); } return null; }
    getMyPeerInfo(): PeerInfo {
        const mode = this.getConnectionMode();
        let port: number | undefined;
        if (mode === 'direct-ip' && this.directIpServer) {
            port = this.settings.directIpHostPort;
        }
        return { 
            deviceId: this.peer?.id || this.settings.deviceId, 
            friendlyName: this.settings.friendlyName, 
            ip: this.getLocalIp(),
            mode: mode,
            port: port
        }; 
    }
    public startDirectIpHost() { if (Platform.isMobile) return; this.reinitializeConnectionManager(); const pin = Array.from(window.crypto.getRandomValues(new Uint8Array(16))).map(b => b.toString(16).padStart(2, '0')).join(''); this.directIpServer = new DirectIpServer(this, this.settings.directIpHostPort, pin); this.updateStatus(); return pin; }
    public async connectToDirectIpHost(config: DirectIpConfig) {
        this.reinitializeConnectionManager();
        this.directIpClient = new DirectIpClient(this, config);
        this.clusterPeers.set('direct-ip-host', { deviceId: 'direct-ip-host', friendlyName: `Host (${config.host})`, ip: config.host });
        
        const mockConn = {
            send: (data: any) => this.directIpClient?.send(data),
            peer: 'direct-ip-host',
            open: true,
            close: () => this.directIpClient?.triggerReconnect()
        } as any;
        this.connections.set('direct-ip-host', mockConn);
        this.updateStatus();

        // Initiate handshake
        await this.directIpClient.send({ type: 'handshake', peerInfo: this.getMyPeerInfo(), pin: config.pin });
    }
    

    public calculateStatus(): SyncStatusState {
        if (this.syncState.isSyncing) {
            let text = "Syncing...";
            if (this.syncState.currentPhase === SyncPhase.REQUESTING) text = "Requesting Sync...";
            else if (this.syncState.currentPhase === SyncPhase.PLANNING) text = "Comparing Vaults...";
            else if (this.syncState.currentPhase === SyncPhase.TRANSFERRING) text = `Syncing (${this.syncState.filesTransferred}/${this.syncState.filesTotal})...`;
            else if (this.syncState.currentPhase === SyncPhase.COMPLETING) text = "Completing Sync...";
            return { text, icon: "refresh-cw", spin: true, state: 'loading' };
        }
        if (this.activeTransfers.size > 0) {
            const count = Array.from(this.activeTransfers.values()).filter(t => t.status === 'active').length;
            if (count === 0 && this.activeTransfers.size > 0) return { text: "Transfers Paused", icon: "pause-circle", state: 'neutral' };
            return { text: `Syncing ${count} file${count > 1 ? 's' : ''}...`, icon: "arrow-up-down", spin: false, state: 'loading' };
        }
        if (this.queueManager.getActiveTransfers() > 0 || this.queueManager.getQueueSize() > 0) {
            const queueSize = this.queueManager.getQueueSize() + this.queueManager.getActiveTransfers();
            return { text: `Syncing (${queueSize} item${queueSize > 1 ? 's' : ''})`, icon: "hourglass", state: 'loading' };
        }
        if (this.getConnectionMode() === 'direct-ip') {
            const isAuto = this.settings.syncMode === 'auto';
            if (this.directIpServer) return { text: isAuto ? "Hosting Offline" : "Host Mode", icon: "server", state: 'success' };
            if (this.directIpClient) {
                const client = this.directIpClient;
                // Fatal error (e.g. PIN rejection) — non-recoverable
                if (client.isFatalError) {
                    return { text: 'Host Rejected PIN', icon: 'shield-off', state: 'error' };
                }
                // Backoff-reconnect in progress
                if (!client.isOpen) {
                    return { text: 'Reconnecting to host…', icon: 'refresh-cw', spin: true, state: 'loading' };
                }
                // Socket open but liveness not yet confirmed (waiting for first message)
                if (!client.isLive) {
                    return { text: 'Verifying link…', icon: 'plug', spin: true, state: 'loading' };
                }
                // Confirmed live connection
                return { text: isAuto ? 'Connected Offline' : 'Client Mode', icon: 'smartphone', state: 'success' };
            }
            return { text: isAuto ? "Offline Mode" : "Offline Mode", icon: "network", state: 'neutral' };
        }
        if (!this.peer || this.peer.disconnected) return { text: "Sync Offline", icon: "wifi-off", state: 'error' };
        if (!this.peer.id) return { text: "Connecting...", icon: "plug", spin: true, state: 'loading' };
        if (this.connections.size > 0) {
            if (this.isTwoDeviceMode()) return { text: "Paired Sync Active", icon: "link", state: 'success' };
            return { text: `Connected (${this.connections.size})`, icon: "users", state: 'success' };
        }
        return { text: "Online", icon: "globe", state: 'neutral' };
    }

    updateStatus(customStatus?: SyncStatusState) {
        const now = Date.now();
        const isIdle = this.activeTransfers.size === 0 && this.queueManager.getActiveTransfers() === 0 && this.queueManager.getQueueSize() === 0;
        if (!customStatus && !isIdle && now - this.lastStatusUpdate < 200) return;
        this.lastStatusUpdate = now;

        const status = customStatus || this.calculateStatus();
        this.statusBar.empty();
        const container = this.statusBar.createDiv({ cls: 'od-status-container' });

        if (status.state === 'loading') {
            container.addClass('mod-clickable');
            container.onclick = () => new SyncProgressModal(this.app, this).open();
        }

        const iconEl = container.createDiv({ cls: 'od-status-icon' });
        setIcon(iconEl, status.icon);
        if (status.spin) iconEl.addClass('lucide-spin');

        if (status.state === 'error') iconEl.style.color = 'var(--text-error)';
        else if (status.state === 'success') iconEl.style.color = 'var(--text-success)';
        else if (status.state === 'loading') iconEl.style.color = 'var(--interactive-accent)';
        else iconEl.style.color = 'var(--text-muted)';

        container.createSpan({ text: status.text });
    }
}


