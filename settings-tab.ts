import { App, PluginSettingTab, Setting, setIcon, Notice } from 'obsidian';
import type ObsidianDecentralizedPlugin from './main';
import { PeerInfo, DEFAULT_SETTINGS, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE } from './types';
import { ConnectionModal, ConfirmModal } from './ui';

export class ObsidianDecentralizedSettingTab extends PluginSettingTab {
    plugin: ObsidianDecentralizedPlugin;
    private clusterStatusEl: HTMLDivElement | null = null;
    private statusInterval: number | null = null;
    private statusTextEl: HTMLDivElement | null = null;
    private isEditingName = false;
    /** Fingerprint of the last rendered status, so the 3 s poll can no-op. */
    private lastStatusSignature: string | null = null;

    constructor(app: App, plugin: ObsidianDecentralizedPlugin) {
        super(app, plugin);
        this.plugin = plugin;
    }

    display(): void {
        this.isEditingName = false;
        // The DOM below is rebuilt from scratch, so the previous render's fingerprint
        // must not suppress the first repaint into the new elements.
        this.lastStatusSignature = null;
        const { containerEl } = this;
        containerEl.empty();
        containerEl.createEl('h2', { text: 'Obsidian Decentralized' });
        const isAuto = this.plugin.settings.syncMode === 'auto';

        const statusContainer = containerEl.createDiv();
        statusContainer.createEl('h3', { text: 'Live Status' });
        this.statusTextEl = statusContainer.createDiv({ cls: 'obsidian-decentralized-status-text' });
        this.statusTextEl.style.fontFamily = 'monospace';
        this.statusTextEl.style.marginBottom = '1em';

        new Setting(containerEl)
            .setName(isAuto ? 'Settings Profile' : 'Mode')
            .setDesc(isAuto ? 'Auto mode is easiest and syncs everything safely. Manual lets you pick folders and behaviors. Advanced unlocks all technical settings.' : 'Auto mode syncs all files. Manual mode allows configuration. Advanced mode unlocks all settings.')
            .addDropdown(dd => dd
                .addOption('auto', 'Auto (Recommended)')
                .addOption('manual', 'Manual')
                .addOption('advanced', 'Advanced')
                .setValue(this.plugin.settings.syncMode)
                .onChange(async (value: 'auto' | 'manual' | 'advanced') => {
                    this.plugin.settings.syncMode = value;
                    await this.plugin.saveSettings();
                    this.display(); 
                }));

        new Setting(containerEl)
            .setName('Connect Devices')
            .setDesc('Open the connection helper to pair with your other devices.')
            .addButton(btn => btn.setButtonText("Connect").setCta().onClick(() => new ConnectionModal(this.app, this.plugin).open()));

        containerEl.createEl('h3', { text: 'Current Cluster' });
        this.clusterStatusEl = containerEl.createDiv();
        this.updateStatus();

        // Shared Settings (Visible in both modes)
        containerEl.createEl('h3', { text: 'Settings' });
        new Setting(containerEl)
            .setName("Excluded folders")
            .setDesc("Never sync folders in this list. One folder per line. Example: 'Attachments/Large Files'.")
            .addTextArea(text => text.setPlaceholder("Path/To/Exclude\nAnother/Path").setValue(this.plugin.settings.excludedFolders).onChange(async (value) => { this.plugin.settings.excludedFolders = value; await this.plugin.saveSettings(); }));

        if (this.plugin.settings.syncMode === 'manual' || this.plugin.settings.syncMode === 'advanced') {
            this.displayManualSettings(containerEl);
        } else {
             new Setting(containerEl)
                .setName('Show Sync Notifications')
                .setDesc('Show notifications for sync events. Important events like connections and conflicts always appear. This toggle controls additional progress notifications.')
                .addToggle(toggle => toggle
                    .setValue(this.plugin.settings.showToasts)
                    .onChange(async (value) => {
                        this.plugin.settings.showToasts = value;
                        await this.plugin.saveSettings();
                    }));
        }
        
        if (this.plugin.settings.syncMode === 'advanced') {
            this.displayAdvancedSettings(containerEl);
        }

        if (this.statusInterval) clearInterval(this.statusInterval);
        this.statusInterval = window.setInterval(() => this.updateStatus(), 3000);
    }

    displayManualSettings(containerEl: HTMLElement): void {
        containerEl.createEl('h4', { text: 'Manual Configuration' });

        new Setting(containerEl)
            .setName('Show Sync Notifications')
            .setDesc('Show notifications for sync events. Important events like connections and conflicts always appear. This toggle controls additional progress notifications.')
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.showToasts)
                .onChange(async (value) => {
                    this.plugin.settings.showToasts = value;
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Conflict Resolution Strategy")
            .setDesc("How to handle a file being edited on two devices at once.")
            .addDropdown(dd => dd
                .addOption('create-conflict-file', 'Create Conflict File (Safest)')
                .addOption('last-write-wins', 'Last Write Wins (Newest file is kept)')
                .setValue(this.plugin.settings.conflictResolutionStrategy)
                .onChange(async (value: 'create-conflict-file' | 'last-write-wins') => {
                    this.plugin.settings.conflictResolutionStrategy = value;
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Sync all file types")
            .setDesc("Sync not just markdown, but also images, PDFs, etc. This will increase sync traffic.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.syncAllFileTypes)
                .onChange(async (value) => {
                    this.plugin.settings.syncAllFileTypes = value;
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Sync '.obsidian' configuration folder")
            .setDesc("DANGEROUS: Syncs settings, themes, and snippets. Can cause issues if devices have different plugins or Obsidian versions. BACKUP FIRST.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.syncObsidianConfig)
                .onChange(async (value) => {
                    this.plugin.settings.syncObsidianConfig = value;
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName('Hide Obsidian Sync status')
            .setDesc('Hide the native Obsidian Sync button in the status bar.')
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.hideNativeSyncStatus)
                .onChange(async (value) => {
                    this.plugin.settings.hideNativeSyncStatus = value;
                    this.plugin.applyHideNativeSync();
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Enable Verbose Logging")
            .setDesc("Outputs detailed sync information to the developer console for troubleshooting.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.verboseLogging)
                .onChange(async (value) => {
                    this.plugin.settings.verboseLogging = value;
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Use custom signaling server")
            .setDesc("For advanced users. Use your own self-hosted PeerJS server for a fully private syncing experience, even over the internet.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.useCustomPeerServer)
                .onChange(async (value) => {
                    this.plugin.settings.useCustomPeerServer = value;
                    await this.plugin.saveSettings();
                    this.display();
                }));

        if (this.plugin.settings.useCustomPeerServer) {
            const config = this.plugin.settings.customPeerServerConfig;
            new Setting(containerEl).setName("Host").addText(text => text.setValue(config.host).onChange(async (value) => { config.host = value; await this.plugin.saveSettings(); }));
            new Setting(containerEl).setName("Port").addText(text => text.setValue(config.port.toString()).onChange(async (value) => {
                const parsed = parseInt(value, 10);
                config.port = isNaN(parsed) ? DEFAULT_SETTINGS.customPeerServerConfig.port : Math.max(1, Math.min(parsed, 65535));
                await this.plugin.saveSettings();
            }));
            new Setting(containerEl).setName("Path").addText(text => text.setValue(config.path).onChange(async (value) => { config.path = value; await this.plugin.saveSettings(); }));
            new Setting(containerEl).setName("Secure (SSL)").addToggle(toggle => toggle.setValue(config.secure).onChange(async (value) => { config.secure = value; await this.plugin.saveSettings(); }));

            new Setting(containerEl)
                .setName("Apply signaling server changes")
                .setDesc("Reconnects using the values above. Current connections drop while it reconnects.")
                .addButton(btn => btn.setButtonText("Apply and reconnect").setWarning()
                    .onClick(() => {
                        this.plugin.showNotice("Reconnecting to the new signaling server…", 'important');
                        this.plugin.reinitializeConnectionManager();
                    }));
        }

        containerEl.createEl('h4', { text: "Selective Sync" });
        new Setting(containerEl)
            .setName("Included folders")
            .setDesc("Only sync folders in this list. One folder per line. If empty, all folders are included (unless excluded). Example: 'Journal/Daily'.")
            .addTextArea(text => text.setPlaceholder("Path/To/Include\nAnother/Path").setValue(this.plugin.settings.includedFolders).onChange(async (value) => { this.plugin.settings.includedFolders = value; await this.plugin.saveSettings(); }));
    }

    displayAdvancedSettings(containerEl: HTMLElement): void {
        containerEl.createEl('h4', { text: 'Two-Device Enhancements' });
        
        new Setting(containerEl)
            .setName("Enable Two-Device Optimizations")
            .setDesc("If exactly one device is connected, enables Version Vectors, Merkle Tree syncing, and Role-based conflict resolution.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.enableTwoDeviceOptimizations)
                .onChange(async (value) => {
                    this.plugin.settings.enableTwoDeviceOptimizations = value;
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Enable End-to-End Encryption")
            .setDesc("Uses AES-GCM encryption with a PSK exchanged during pairing. Highly recommended.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.enableEncryption)
                .onChange(async (value) => {
                    this.plugin.settings.enableEncryption = value;
                    await this.plugin.saveSettings();
                }));

        containerEl.createEl('h4', { text: 'Advanced Settings' });

        new Setting(containerEl)
            .setName("Turbo Real-time")
            .setDesc("WARNING: Streams live keystrokes instantly to avoid conflicts. Requires a flawless connection and a strict 2-device setup. Can be destructive if misused.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.enableRealtimeSync)
                .onChange(async (value) => {
                    this.plugin.settings.enableRealtimeSync = value;
                    await this.plugin.saveSettings();
                }));
        
        new Setting(containerEl)
            .setName("Enable Text Compression")
            .setDesc("Compress text files (markdown, css, etc.) before sending. Significantly reduces bandwidth usage.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.enableCompression)
                .onChange(async (value) => {
                    this.plugin.settings.enableCompression = value;
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Enable Delta Sync")
            .setDesc("Only send changes (deltas) instead of the whole file when a text file is modified. Speeds up syncing for large notes.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.enableDeltaSync)
                .onChange(async (value) => {
                    this.plugin.settings.enableDeltaSync = value;
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Delta Sync Threshold (%)")
            .setDesc("Only use delta sync if the patch is smaller than this percentage of the total file size.")
            .addText(text => text
                .setValue(this.plugin.settings.deltaSyncThreshold.toString())
                .onChange(async (value) => {
                    const num = parseInt(value);
                    this.plugin.settings.deltaSyncThreshold = isNaN(num) ? DEFAULT_SETTINGS.deltaSyncThreshold : Math.max(0, Math.min(num, 100));
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Strict security")
            .setDesc("Only accept devices this vault has an encryption key for, and reject unencrypted messages from them. Recommended, but devices paired by typing a bare device ID will need to be paired again using the full copied code or the QR.")
            .addToggle(toggle => toggle
                .setValue(this.plugin.settings.strictSecurity)
                .onChange(async (value) => {
                    this.plugin.settings.strictSecurity = value;
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Idle Timeout (ms)")
            .setDesc("Maximum time to wait for a sync operation without progress before aborting.")
            .addText(text => text
                .setValue(this.plugin.settings.idleTimeoutMs?.toString() || "30000")
                .onChange(async (value) => {
                    const num = parseInt(value);
                    this.plugin.settings.idleTimeoutMs = isNaN(num) ? DEFAULT_SETTINGS.idleTimeoutMs : Math.max(5000, Math.min(num, 600000));
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Tombstone Retention (Days)")
            .setDesc("How long to remember deleted files. Peers offline longer than this might resurrect deleted files.")
            .addText(text => text
                .setValue(this.plugin.settings.tombstoneRetentionDays?.toString() || "30")
                .onChange(async (value) => {
                    const num = parseInt(value);
                    this.plugin.settings.tombstoneRetentionDays = isNaN(num) ? DEFAULT_SETTINGS.tombstoneRetentionDays : Math.max(0, Math.min(num, 3650));
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Max Concurrent Transfers")
            .setDesc("Maximum number of files to transfer at once. Leave empty for dynamic (auto-tuning).")
            .addText(text => text
                .setPlaceholder("Auto")
                .setValue(this.plugin.settings.maximumConcurrentTransfers?.toString() || "")
                .onChange(async (value) => {
                    const num = parseInt(value);
                    this.plugin.settings.maximumConcurrentTransfers = isNaN(num) ? null : Math.max(1, Math.min(num, 100));
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Chunk Size (Bytes)")
            .setDesc("Size of file chunks in bytes. Default is dynamic (starts at 64KB).")
            .addText(text => text
                .setPlaceholder("Auto")
                .setValue(this.plugin.settings.chunkSize?.toString() || "")
                .onChange(async (value) => {
                    const num = parseInt(value);
                    this.plugin.settings.chunkSize = isNaN(num) ? null : Math.max(MIN_CHUNK_SIZE, Math.min(num, MAX_CHUNK_SIZE));
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Debounce Delay (ms)")
            .setDesc("Time to wait after a file change before syncing. Higher values reduce sync frequency.")
            .addText(text => text
                .setValue(this.plugin.settings.debounceDelay.toString())
                .onChange(async (value) => {
                    const num = parseInt(value);
                    this.plugin.settings.debounceDelay = isNaN(num) ? DEFAULT_SETTINGS.debounceDelay : Math.max(250, num);
                    this.plugin.updateDebounceDelay();
                    await this.plugin.saveSettings();
                }));

        new Setting(containerEl)
            .setName("Modification Time Tolerance (ms)")
            .setDesc("Time difference to consider files 'the same' to account for clock skew.")
            .addText(text => text
                .setValue(this.plugin.settings.mtimeTolerance.toString())
                .onChange(async (value) => {
                    const num = parseInt(value);
                    this.plugin.settings.mtimeTolerance = isNaN(num) ? DEFAULT_SETTINGS.mtimeTolerance : Math.max(0, num);
                    await this.plugin.saveSettings();
                }));
    }

    hide(): void {
        if (this.statusInterval) {
            clearInterval(this.statusInterval);
            this.statusInterval = null;
        }
    }

    private createEditableName(container: HTMLElement, displayName: string, editValue: string, onSave: (newName: string) => void) {
        container.empty();
        const wrapper = container.createDiv({ cls: 'od-editable-name-container' });
        
        wrapper.createSpan({ text: displayName, cls: 'od-editable-name-text' });
        const iconEl = wrapper.createSpan({ cls: 'od-editable-name-icon' });
        setIcon(iconEl, 'pencil');

        const switchToEdit = () => {
            this.isEditingName = true;
            wrapper.empty();
            wrapper.removeClass('od-editable-name-container'); 
            wrapper.addClass('od-setting-item-name-wrapper');

            const inputEl = wrapper.createEl('input', { type: 'text', value: editValue });
            inputEl.addClass('od-editable-name-input');
            
            const save = () => {
                this.isEditingName = false;
                const newName = inputEl.value.trim();
                if (newName && newName.length <= 64) {
                    if (newName !== editValue) {
                        onSave(newName);
                    } else {
                        this.createEditableName(container, displayName, editValue, onSave);
                    }
                } else {
                    new Notice(newName === "" ? "Name cannot be empty." : "Name cannot exceed 64 characters.", 5000);
                    this.createEditableName(container, displayName, editValue, onSave);
                }
            };

            const submitBtn = wrapper.createSpan({ cls: 'od-editable-name-submit' });
            setIcon(submitBtn, 'check');
            submitBtn.onclick = (e) => { e.stopPropagation(); save(); };

            inputEl.onkeydown = (e) => { 
                if (e.key === 'Enter') save(); 
                if (e.key === 'Escape') { this.isEditingName = false; this.createEditableName(container, displayName, editValue, onSave); }
            };
            inputEl.onclick = (e) => e.stopPropagation();
            
            setTimeout(() => inputEl.focus(), 0);
        };
        wrapper.onclick = (e) => { e.preventDefault(); switchToEdit(); };
    }

    updateStatus() {
        // Polled every 3 s while the tab is open. Both sections below are full DOM
        // rebuilds (setIcon parses an SVG; the cluster list constructs a Setting per
        // peer), so skip the work entirely when nothing on screen would change.
        const status = this.plugin.calculateStatus();
        const peers = Array.from(this.plugin.clusterPeers.values());
        const signature = [
            status.text, status.icon, status.state, status.spin ? '1' : '0',
            peers.map(p => `${p.deviceId}:${p.friendlyName}:${this.plugin.connections.has(p.deviceId) ? 1 : 0}`).join(','),
            this.plugin.settings.companionPeerId ?? '',
        ].join('|');

        if (signature === this.lastStatusSignature) return;
        this.lastStatusSignature = signature;

        if (this.statusTextEl) {
            this.statusTextEl.empty();
            const iconSpan = this.statusTextEl.createSpan({ cls: 'od-status-icon' });
            setIcon(iconSpan, status.icon);
            if (status.spin) iconSpan.addClass('lucide-spin');
            this.statusTextEl.createSpan({ text: status.text });
        }

        if (!this.clusterStatusEl || !this.clusterStatusEl.isConnected) {
            return;
        }
        if (this.isEditingName) {
            // Nothing was rendered, so don't let the signature suppress the next pass.
            this.lastStatusSignature = null;
            return;
        }
        this.clusterStatusEl.empty();

        const createEntry = (peer: PeerInfo, type: 'self' | 'companion' | 'peer' | 'host' | 'disconnected') => {
            const settingItem = new Setting(this.clusterStatusEl!)
                .setDesc(`ID: ${peer.deviceId}`);

            if (type === 'self') {
                this.createEditableName(settingItem.nameEl, peer.friendlyName + ' (This Device)', peer.friendlyName, async (newName) => {
                    this.plugin.settings.friendlyName = newName;
                    await this.plugin.saveSettings();
                    this.plugin.broadcastData({ type: 'cluster-rename', targetDeviceId: this.plugin.settings.deviceId, newName });
                    this.updateStatus();
                });
            } else if (type === 'peer' || type === 'disconnected') {
                this.createEditableName(settingItem.nameEl, peer.friendlyName, peer.friendlyName, (newName) => {
                    this.plugin.broadcastData({ type: 'cluster-rename', targetDeviceId: peer.deviceId, newName });
                    peer.friendlyName = newName;
                    this.plugin.saveKnownPeers();
                    this.updateStatus();
                });
            } else {
                settingItem.setName(peer.friendlyName);
            }

            if (type === 'companion') {
                settingItem.addButton(btn => btn.setButtonText('Unpair').setWarning().onClick(() => {
                    new ConfirmModal(this.app, {
                        title: 'Unpair this device?',
                        body: `${peer.friendlyName} will no longer be your primary sync partner. You can pair again later.`,
                        confirmText: 'Unpair',
                        onConfirm: async () => {
                            await this.plugin.forgetCompanion();
                            this.updateStatus();
                        },
                    }).open();
                }));
            }
            if (type === 'peer' || type === 'disconnected') {
                settingItem.addExtraButton(btn => btn
                    .setIcon('star')
                    .setTooltip('Set as Primary Sync Partner')
                    .onClick(async () => {
                        this.plugin.settings.companionPeerId = peer.deviceId;
                        await this.plugin.saveSettings();
                        // Tell the other device it is now paired with us — without this
                        // message the pairing was one-sided and the peer never knew.
                        this.plugin.sendData(peer.deviceId, { type: 'companion-pair', peerInfo: this.plugin.getMyPeerInfo() });
                        this.plugin.tryToConnectToClusterPeers();
                        this.updateStatus();
                    })
                );
            }
            if (type === 'peer' || type === 'companion' || type === 'disconnected') {
                const conn = this.plugin.connections.get(peer.deviceId);
                if (conn && conn.open) {
                    settingItem.addButton(btn => btn.setButtonText('Disconnect').onClick(() => {
                        conn.close();
                        this.plugin.showNotice(`Disconnecting from ${peer.friendlyName}`, 'important');
                        setTimeout(() => this.updateStatus(), 100);
                    }));
                    settingItem.addExtraButton(btn => btn.setIcon('trash').setTooltip('Kick Device').onClick(() => {
                        new ConfirmModal(this.app, {
                            title: `Remove ${peer.friendlyName} from the group?`,
                            body: 'Every device in the group stops syncing with it. It can rejoin by pairing again.',
                            confirmText: 'Remove device',
                            onConfirm: () => {
                                this.plugin.broadcastData({ type: 'cluster-kick', targetDeviceId: peer.deviceId });
                                if (this.plugin.connections.has(peer.deviceId)) this.plugin.connections.get(peer.deviceId)?.close();
                                setTimeout(() => this.updateStatus(), 100);
                            },
                        }).open();
                    }));
                    settingItem.addExtraButton(btn => btn.setIcon('activity').setTooltip('Ping').onClick(() => {
                        this.plugin.manualPingStart.set(peer.deviceId, Date.now());
                        conn.send({ type: 'ping' });
                    }));
                } else {
                    settingItem.setDesc((settingItem.descEl.textContent ?? '') + ' (Disconnected)');
                    settingItem.nameEl.style.color = 'var(--text-muted)';
                    
                    settingItem.addButton(btn => btn.setButtonText('Reconnect').setCta().onClick(() => {
                        if (this.plugin.peer && !this.plugin.peer.disconnected) {
                            this.plugin.showNotice(`Reconnecting to ${peer.friendlyName}...`, 'important');
                            const newConn = this.plugin.peer.connect(peer.deviceId, { reliable: true });
                            this.plugin.setupConnection(newConn);
                        } else {
                            this.plugin.showNotice("Cannot reconnect: Your sync is offline.", 'error');
                        }
                    }));
                    if (type !== 'companion') {
                        settingItem.addButton(btn => btn.setButtonText('Forget').setWarning().onClick(() => {
                            new ConfirmModal(this.app, {
                                title: `Forget ${peer.friendlyName}?`,
                                body: 'This device is removed from your saved devices and its encryption key is deleted. You will need to pair again to sync with it.',
                                confirmText: 'Forget device',
                                onConfirm: () => {
                                    this.plugin.pendingConnections.delete(peer.deviceId);
                                    this.plugin.broadcastData({ type: 'cluster-forget', targetDeviceId: peer.deviceId });
                                    this.plugin.handleClusterForget({ type: 'cluster-forget', targetDeviceId: peer.deviceId });
                                    // Forgetting a device must also drop its pre-shared key; leaving it
                                    // behind meant a "forgotten" peer could still be auto-trusted later.
                                    delete this.plugin.settings.peerKeys[peer.deviceId];
                                    this.plugin.invalidateCryptoKey(peer.deviceId);
                                    void this.plugin.saveSettings();
                                    this.plugin.saveKnownPeers();
                                    setTimeout(() => this.updateStatus(), 100);
                                },
                            }).open();
                        }));
                    }
                }
            } else if (type === 'host') {
                settingItem.addButton(btn => btn.setButtonText('Disconnect').onClick(() => {
                    this.plugin.reinitializeConnectionManager();
                    setTimeout(() => this.updateStatus(), 100);
                }));
            }
        };
        createEntry(this.plugin.getMyPeerInfo(), 'self');

        if (this.plugin.getConnectionMode() === 'direct-ip') {
            const list = this.clusterStatusEl;
            if (this.plugin.directIpServer) {
                list.createEl('p', { text: 'Hosting on Offline Mode. Other devices can connect to you.' });
            } else if (this.plugin.directIpClient) {
                // Look up the actual host entry — taking the FIRST clusterPeers value
                // showed a stale, unrelated known peer as the connected host.
                const hostInfo = this.plugin.clusterPeers.get('direct-ip-host');
                if (hostInfo) createEntry(hostInfo, 'host');
            } else {
                list.createEl('p', { text: 'Offline Mode is idle. Use the "Connect" button to start.' });
            }
            return;
        }

        const companionId = this.plugin.settings.companionPeerId;
        if (companionId && this.plugin.clusterPeers.has(companionId)) {
            createEntry(this.plugin.clusterPeers.get(companionId)!, 'companion');
        }
        this.plugin.clusterPeers.forEach(peer => {
            if (peer.deviceId !== companionId) {
                const isConnected = this.plugin.connections.has(peer.deviceId) && this.plugin.connections.get(peer.deviceId)?.open;
                createEntry(peer, isConnected ? 'peer' : 'disconnected');
            }
        });

        if (this.plugin.clusterPeers.size === 0) {
            const list = this.clusterStatusEl;
            if (!this.plugin.peer || this.plugin.peer.disconnected) {
                list.createEl('p', { text: 'Sync is offline. Trying to reconnect...' });
            } else if (!this.plugin.peer.id) {
                list.createEl('p', { text: 'Connecting to sync network...' });
            } else {
                list.createEl('p', { text: 'Not connected to any other devices.' });
            }
        }
    }
}
