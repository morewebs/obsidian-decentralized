import { App, Modal, Platform, Setting, TFile, Notice, setIcon } from 'obsidian';
import { DataConnection } from 'peerjs';
import DiffMatchPatch from 'diff-match-patch';
import type * as QRCodeType from 'qrcode';
import type { Html5Qrcode as Html5QrcodeType } from 'html5-qrcode';
import type ObsidianDecentralizedPlugin from './main';
import { PeerInfo, describeSyncPhase } from './types';

// The QR generator and scanner together account for over half the bundle, yet they
// are only reachable from the pairing modal. Importing them dynamically keeps their
// module-level initialisation (notably html5-qrcode's ZXing tables) off the plugin
// startup path — it now runs the first time a user actually opens pairing.
let qrCodeModule: typeof QRCodeType | null = null;
async function loadQRCode(): Promise<typeof QRCodeType> {
    if (!qrCodeModule) qrCodeModule = await import('qrcode');
    return qrCodeModule;
}

let html5QrcodeCtor: typeof Html5QrcodeType | null = null;
async function loadHtml5Qrcode(): Promise<typeof Html5QrcodeType> {
    if (!html5QrcodeCtor) html5QrcodeCtor = (await import('html5-qrcode')).Html5Qrcode;
    return html5QrcodeCtor;
}

export function formatBytes(bytes: number, decimals = 2) {
    if (!Number.isFinite(bytes) || bytes <= 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.max(0, Math.min(sizes.length - 1, Math.floor(Math.log(bytes) / Math.log(k))));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

export class ConnectionModal extends Modal {
    private discoveredPeers: Map<string, PeerInfo> = new Map();
    private discoverListener: ((p: PeerInfo) => void) | null = null;
    private loseListener: ((p: PeerInfo) => void) | null = null;
    
    private activeTab: 'quick-pair' | 'advanced' = 'quick-pair';
    private statusState: 'idle' | 'connecting' | 'reconnecting' | 'connected' | 'error' = 'idle';
    private statusMessage: string = 'Not connected';
    private activePsk: string | null = null;

    constructor(app: App, private plugin: ObsidianDecentralizedPlugin) { super(app); }

    async onOpen() {
        this.contentEl.addClass(Platform.isMobile ? 'od-mobile' : 'od-desktop');
        
        if (this.plugin.connections && this.plugin.connections.size > 0) {
            // Direct IP: reflect liveness/reconnect state precisely
            const client = this.plugin.directIpClient;
            if (client) {
                if (client.isFatalError) {
                    this.statusState = 'error';
                    this.statusMessage = 'Host rejected PIN — cannot reconnect.';
                } else if (!client.isOpen) {
                    this.statusState = 'reconnecting';
                    this.statusMessage = 'Reconnecting to Offline Host…';
                } else if (!client.isLive) {
                    this.statusState = 'connecting';
                    this.statusMessage = 'Verifying link to Offline Host…';
                } else {
                    this.statusState = 'connected';
                    this.statusMessage = `Connected to ${this.plugin.connections.size} device(s)`;
                }
            } else {
                this.statusState = 'connected';
                this.statusMessage = `Connected to ${this.plugin.connections.size} device(s)`;
            }
        }
        
        // Opening this modal is what puts the device into pairing mode, so (re)start the
        // window here rather than leaving auto-enrolment armed for the whole session.
        this.activePsk = await this.plugin.beginPairingWindow();
        
        this.render();
    }

    onClose() {
        if (this.plugin.lanDiscovery) {
            if (this.discoverListener) this.plugin.lanDiscovery.off('discover', this.discoverListener);
            if (this.loseListener) this.plugin.lanDiscovery.off('lose', this.loseListener);
            // Do NOT stopBroadcasting() here: the beacon is global (started at plugin
            // load) — stopping it on modal close made this device undiscoverable on
            // the LAN until the plugin was reloaded.
        }
        this.contentEl.empty();
    }


    formatPairingCodeForDisplay(deviceId: string): string {
        if (deviceId.startsWith('device-')) {
            const hex = deviceId.substring(7);
            if (hex.length >= 8) {
                const mid = Math.floor(hex.length / 2);
                return `device-${hex.substring(0, mid)}-${hex.substring(mid)}`;
            }
        }
        return deviceId;
    }

    normalizePairingCodeInput(input: string): string {
        let cleaned = input.trim().replace(/\s+/g, '');
        if (cleaned.startsWith('device-')) {
            const hexPart = cleaned.substring(7).replace(/-/g, '');
            return `device-${hexPart}`;
        }
        const justHex = cleaned.replace(/-/g, '');
        if (justHex.length === 8 && /^[0-9a-fA-F]{8}$/.test(justHex)) {
            return `device-${justHex.toLowerCase()}`;
        }
        return cleaned;
    }

    render() {
        this.contentEl.empty();
        this.renderStatusBanner();
        this.renderTabNavigation();
        
        if (this.activeTab === 'quick-pair') {
            this.renderQuickPairTab();
        } else {
            this.renderAdvancedTab();
        }
    }

    renderStatusBanner() {
        const banner = this.contentEl.createDiv({ cls: `od-status-banner ${this.statusState}` });
        
        if (this.statusState === 'connecting') {
            const spinner = banner.createSpan();
            setIcon(spinner, 'loader');
            spinner.addClass('lucide-spin');
        } else if (this.statusState === 'reconnecting') {
            // Amber spinning indicator — connection was previously established but lost
            const spinner = banner.createSpan();
            setIcon(spinner, 'refresh-cw');
            spinner.addClass('lucide-spin');
        } else if (this.statusState === 'connected') {
            // Only shown after liveness is confirmed (Phase 4)
            const check = banner.createSpan();
            setIcon(check, 'check-circle');
        } else if (this.statusState === 'error') {
            // Only shown for non-recoverable failures (e.g. PIN rejection)
            const alert = banner.createSpan();
            setIcon(alert, 'alert-triangle');
        }
        
        banner.createSpan({ text: this.statusMessage });
    }

    renderTabNavigation() {
        const tabsContainer = this.contentEl.createDiv({ cls: 'od-connection-tabs' });
        
        const quickPairBtn = tabsContainer.createEl('button', { text: 'Quick Pair', cls: `od-tab-btn ${this.activeTab === 'quick-pair' ? 'active' : ''}` });
        quickPairBtn.onclick = () => { this.activeTab = 'quick-pair'; this.render(); };
        
        const advancedBtn = tabsContainer.createEl('button', { text: 'Advanced', cls: `od-tab-btn ${this.activeTab === 'advanced' ? 'active' : ''}` });
        advancedBtn.onclick = () => { this.activeTab = 'advanced'; this.render(); };
    }

    async attemptConnection(scannedId: string) {
        if (!scannedId.trim()) return;
        
        const parts = scannedId.split('|');
        const peerId = this.normalizePairingCodeInput(parts[0]);
        const psk = parts[1];

        if (psk) {
            // Validate before storing. A malformed key used to be written straight into
            // settings and only failed later, deep inside getCryptoKey, on every message.
            if (!/^[A-Za-z0-9+/]{40,}={0,2}$/.test(psk)) {
                this.statusState = 'error';
                this.statusMessage = 'That pairing code looks damaged. Copy it again from the other device.';
                this.render();
                return;
            }
            this.plugin.settings.peerKeys[peerId] = psk;
            await this.plugin.saveSettings();
        } else {
            this.plugin.showNotice(
                'Connecting without an encryption key — this device ID was entered on its own. Use the full copied code or the QR to encrypt the link.',
                'warning'
            );
        }

        this.statusState = 'connecting';
        this.statusMessage = `Connecting to ${peerId}...`;
        this.render();

        // reliable:true is required — an unordered channel lets file-chunk-data
        // overtake file-chunk-start, permanently breaking large-file transfers.
        const conn = this.plugin.peer?.connect(peerId, { reliable: true });
        if (!conn) {
            this.statusState = 'error';
            this.statusMessage = 'Failed to initiate connection. Are you online?';
            this.render();
            return;
        }

        // Register the plugin's handlers BEFORE 'open' fires. setupConnection attaches
        // its own 'open' listener (which sends the handshake); registering it lazily
        // inside our own open handler missed the event, so the handshake was never
        // sent and pairing produced a half-open one-way connection.
        this.plugin.setupConnection(conn);

        // PeerJS reports "peer unavailable" on the Peer object, not this connection —
        // without a timeout a typo'd code left the modal spinning forever.
        const connectTimeout = window.setTimeout(() => {
            if (this.statusState === 'connecting') {
                this.statusState = 'error';
                this.statusMessage = 'Connection timed out. Check the code and make sure the other device is online.';
                this.render();
            }
        }, 20000);

        conn.on('open', () => {
            window.clearTimeout(connectTimeout);
            this.statusState = 'connected';
            this.statusMessage = `Connected to ${conn.peer}`;
            this.render();
            setTimeout(() => this.close(), 1500);
        });

        conn.on('error', (err) => {
            window.clearTimeout(connectTimeout);
            this.statusState = 'error';
            this.statusMessage = `Connection failed. Try again.`;
            this.render();
        });
    }

    renderQuickPairTab() {
        const { contentEl } = this;
        contentEl.createEl('h2', { text: 'Quick Pair', cls: 'od-dashboard-header' });
        contentEl.createDiv({ text: 'Connect devices easily using codes or QR', cls: 'od-dashboard-subtitle' });

        const myInfo = this.plugin.getMyPeerInfo();
        if (!myInfo || !myInfo.deviceId) {
            contentEl.createDiv({ text: 'Error: This device does not have a valid Device ID. Please check settings.', cls: 'mod-warning' });
            return;
        }

        // Step 1: Share Your Code
        contentEl.createDiv({ text: 'Step 1: Share Your Code', cls: 'od-step-header' });
        
        const codeContainer = contentEl.createDiv({ cls: 'od-pairing-code-container' });
        const formattedCode = this.formatPairingCodeForDisplay(myInfo.deviceId);
        codeContainer.createDiv({ text: formattedCode, cls: 'od-pairing-code-text' });
        
        if (!this.activePsk) {
            contentEl.createDiv({ text: 'Error: Encryption key (PSK) is missing. Pairing is blocked.', cls: 'mod-warning' });
            new Notice('Pairing blocked: Encryption key (PSK) is missing or not generated.');
            return;
        }
        
        const qrPayload = `${myInfo.deviceId}|${this.activePsk}`;
        
        const copyBtn = codeContainer.createEl('button', { text: 'Copy' });
        copyBtn.onclick = () => {
            navigator.clipboard.writeText(qrPayload);
            copyBtn.setText('Copied!');
            setTimeout(() => copyBtn.setText('Copy'), 2000);
        };

        // The code on screen is only the device ID; Copy puts the device ID *and* the
        // encryption key on the clipboard. Say so — users were being told to share it.
        contentEl.createDiv({
            text: 'Press Copy and paste the result on your other device. The copied code contains this vault\'s encryption key, so treat it like a password — anyone who has it can pair with you.',
            cls: 'od-instruction-text'
        });
        
        const qrSection = contentEl.createDiv({ cls: 'od-qr-section' });
        qrSection.createDiv({ text: 'Or scan QR code (Includes Encryption Key)', cls: 'od-qr-label' });
        
        const imgEl = qrSection.createEl('img');
        loadQRCode()
            .then(QRCode => QRCode.toDataURL(qrPayload, { width: 150, margin: 2 }))
            .then(url => {
                imgEl.src = url;
            }).catch(err => {
                imgEl.remove();
                qrSection.createEl('p', { text: 'Failed to load QR code.', cls: 'od-text-muted' });
            });
        
        const scanBtn = qrSection.createEl('button', { text: 'Scan QR Code', cls: 'od-full-width' });
        scanBtn.onclick = () => {
            new QRScannerModal(this.app, (scannedId) => {
                this.attemptConnection(scannedId);
            }).open();
        };

        contentEl.createDiv({ cls: 'od-section-divider' });

        // Step 2: Enter Their Code
        contentEl.createDiv({ text: 'Step 2: Enter Their Code', cls: 'od-step-header' });
        
        const inputRow = contentEl.createDiv({ cls: 'od-input-row' });
        const input = inputRow.createEl('input', { type: 'text', placeholder: 'Enter their pairing code or paste full key' });
        
        const connectBtn = inputRow.createEl('button', { text: 'Connect', cls: 'mod-cta' });
        connectBtn.onclick = () => {
            this.attemptConnection(input.value);
        };

        // LAN Discovery
        if (!Platform.isMobile) {
            contentEl.createDiv({ cls: 'od-section-divider' });
            contentEl.createDiv({ text: 'Or connect to nearby devices', cls: 'od-step-header' });
            
            const lanList = contentEl.createDiv({ cls: 'od-peer-list' });
            
            const renderLanList = () => {
                lanList.empty();
                if (this.discoveredPeers.size === 0) {
                    const emptyState = lanList.createDiv({ cls: 'od-scanning' });
                    emptyState.createSpan({ cls: 'od-pulsing-indicator' });
                    emptyState.createSpan({ text: 'Scanning for devices...' });
                } else {
                    this.discoveredPeers.forEach((peer) => {
                        const card = lanList.createDiv({ cls: 'od-lan-card mod-clickable' });
                        card.createDiv({ text: peer.friendlyName, cls: 'od-peer-name' });
                        card.createDiv({ text: Platform.isMobile ? 'Tap to connect' : 'Click to connect', cls: 'od-text-muted' });
                        card.onclick = () => this.attemptConnection(peer.deviceId);
                    });
                }
            };
            
            if (this.discoverListener && this.plugin.lanDiscovery) this.plugin.lanDiscovery.off('discover', this.discoverListener);
            if (this.loseListener && this.plugin.lanDiscovery) this.plugin.lanDiscovery.off('lose', this.loseListener);

            this.discoverListener = (p: PeerInfo) => { this.discoveredPeers.set(p.deviceId, p); renderLanList(); };
            this.loseListener = (p: PeerInfo) => { this.discoveredPeers.delete(p.deviceId); renderLanList(); };

            if (this.plugin.lanDiscovery) {
                this.plugin.lanDiscovery.on('discover', this.discoverListener);
                this.plugin.lanDiscovery.on('lose', this.loseListener);
                this.plugin.lanDiscovery.startBroadcasting(this.plugin.getMyPeerInfo());
                this.plugin.lanDiscovery.startListening();
                // Seed with peers discovered BEFORE this modal opened — their
                // 'discover' events fired long ago and won't fire again.
                for (const p of this.plugin.lanDiscovery.getDiscoveredPeers()) {
                    this.discoveredPeers.set(p.deviceId, p);
                }
            }
            renderLanList();
        }
    }

    renderAdvancedTab() {
        const { contentEl } = this;

        if (this.plugin.settings.connectionMode === 'direct-ip') {
            this.renderDirectIpDashboard();
            return;
        }

        contentEl.createEl('h2', { text: 'Advanced Configuration', cls: 'od-dashboard-header' });
        contentEl.createDiv({ text: 'Manual connection options and settings', cls: 'od-dashboard-subtitle' });

        // Offline Mode Switch
        contentEl.createDiv({ cls: 'od-section-title', text: 'Offline Mode (Direct IP)' });
        contentEl.createEl('p', { text: 'Use Offline Mode when you have no internet access but are on the same local network.', cls: 'od-text-muted', attr: { style: 'font-size: 0.85em;' } });
        
        const footer = contentEl.createDiv({ cls: 'od-mode-switch', attr: { style: 'margin-top: 10px;' } });
        footer.setText("Switch to Offline Mode");
        footer.onclick = async () => {
            this.plugin.settings.connectionMode = 'direct-ip';
            await this.plugin.saveSettings();
            this.plugin.reinitializeConnectionManager();
            this.render();
        };
    }

    renderDirectIpDashboard() {
        const { contentEl } = this;
        contentEl.createEl('h2', { text: 'Offline Mode', cls: 'od-dashboard-header' });
        contentEl.createDiv({ text: 'Connect directly over your local network', cls: 'od-dashboard-subtitle' });
        
        const container = contentEl.createDiv({ cls: 'od-direct-ip-wrapper' });

        container.createDiv({ cls: 'od-section-title', text: 'Step 1: Host a Network (Main device)' });
        if (!Platform.isMobile) {
            const hostBtn = container.createEl('button', { text: 'Start Hosting', cls: 'mod-cta od-full-width' });
            hostBtn.onclick = async () => {
                hostBtn.disabled = true;
                hostBtn.setText('Starting…');
                let pin: string | null = null;
                try {
                    pin = await this.plugin.startDirectIpHost();
                } finally {
                    hostBtn.disabled = false;
                    hostBtn.setText('Start Hosting');
                }
                // A null token means the socket never bound; startDirectIpHost has already
                // explained why, so don't paint a "Hosting Active" screen over the failure.
                if (!pin) return;

                const ip = this.plugin.getLocalIp();
                contentEl.empty();
                contentEl.createEl('h2', { text: 'Hosting Active', cls: 'od-dashboard-header' });
                contentEl.createDiv({
                    text: ip ? `IP: ${ip}` : 'No network address found — check that you are connected to Wi-Fi or Ethernet.',
                    cls: 'od-ip-display'
                });
                contentEl.createDiv({ text: `Token: ${pin}`, cls: 'od-pin-display od-token' });
                const copyBtn = contentEl.createEl('button', { text: 'Copy token', cls: 'od-full-width od-spaced-top' });
                copyBtn.onclick = async () => {
                    await navigator.clipboard.writeText(pin!);
                    copyBtn.setText('Copied');
                    setTimeout(() => copyBtn.setText('Copy token'), 1500);
                };
                contentEl.createEl('button', { text: 'Done', cls: 'od-full-width od-spaced-top' }).onclick = () => this.close();
            };
        } else {
            container.createDiv({ text: 'Hosting is not available on mobile.', cls: 'od-text-muted' });
        }

        if (!Platform.isMobile) {
            container.createDiv({ cls: 'od-section-title', text: 'Discovered Hosts' });
            const lanList = container.createDiv({ cls: 'od-peer-list' });
            const renderLanList = () => {
                lanList.empty();
                if (this.discoveredPeers.size === 0) {
                    const emptyState = lanList.createDiv({ cls: 'od-scanning' });
                    emptyState.createSpan({ cls: 'od-pulsing-indicator' });
                    emptyState.createSpan({ text: 'Scanning for hosts...' });
                } else {
                    this.discoveredPeers.forEach((peer) => {
                        const item = lanList.createDiv({ cls: 'od-peer-item' });
                        const info = item.createDiv({ cls: 'info' });
                        info.createDiv({ text: peer.friendlyName, cls: 'od-peer-name' });
                        info.createDiv({ text: `${peer.ip || 'Unknown IP'}:${peer.port || '???'}`, cls: 'sub-text' });
                        const btn = item.createEl('button', { text: 'Select' });
                        btn.onclick = async () => {
                            ipInput.value = peer.ip || '';
                            if (peer.port) { this.plugin.settings.directIpHostPort = peer.port; await this.plugin.saveSettings(); }
                            new Notice(`Selected ${peer.friendlyName}`);
                        };
                    });
                }
            };
            
            if (this.discoverListener && this.plugin.lanDiscovery) this.plugin.lanDiscovery.off('discover', this.discoverListener);
            if (this.loseListener && this.plugin.lanDiscovery) this.plugin.lanDiscovery.off('lose', this.loseListener);

            this.discoverListener = (p: PeerInfo) => { this.discoveredPeers.set(p.deviceId, p); renderLanList(); };
            this.loseListener = (p: PeerInfo) => { this.discoveredPeers.delete(p.deviceId); renderLanList(); };

            if (this.plugin.lanDiscovery) {
                this.plugin.lanDiscovery.on('discover', this.discoverListener);
                this.plugin.lanDiscovery.on('lose', this.loseListener);
                this.plugin.lanDiscovery.startBroadcasting(this.plugin.getMyPeerInfo());
                this.plugin.lanDiscovery.startListening();
                // Seed with peers discovered BEFORE this view opened
                for (const p of this.plugin.lanDiscovery.getDiscoveredPeers()) {
                    this.discoveredPeers.set(p.deviceId, p);
                }
            }
            renderLanList();
        }

        container.createDiv({ cls: 'od-section-divider' });

        container.createDiv({ cls: 'od-section-title', text: 'Step 2: Join a Network (Other devices)' });
        const ipInput = container.createEl('input', { type: 'text', placeholder: 'Host IP Address' });
        ipInput.value = this.plugin.settings.directIpHostAddress || '';
        if (Platform.isMobile) { ipInput.style.width = '100%'; ipInput.style.marginBottom = '10px'; }
        
        const pinInput = container.createEl('input', { type: 'text', placeholder: 'Security Token' });
        if (Platform.isMobile) { pinInput.style.width = '100%'; pinInput.style.marginBottom = '10px'; }

        const connectBtn = container.createEl('button', { text: 'Connect', cls: 'mod-cta od-full-width' });
        connectBtn.onclick = async () => {
            if (ipInput.value && pinInput.value) {
                this.plugin.settings.directIpHostAddress = ipInput.value;
                await this.plugin.saveSettings();
                this.plugin.connectToDirectIpHost({ host: ipInput.value, port: this.plugin.settings.directIpHostPort, pin: pinInput.value });
                this.close();
            } else {
                new Notice('Please provide both IP and Token');
            }
        };

        const footer = contentEl.createDiv({ cls: 'od-mode-switch' });
        footer.setText("Switch to Standard Mode");
        footer.onclick = async () => {
            this.plugin.settings.connectionMode = 'peerjs';
            await this.plugin.saveSettings();
            this.plugin.reinitializeConnectionManager();
            this.render();
        };
    }
}

export class QRScannerModal extends Modal {
    private html5QrCode: Html5QrcodeType | null = null;
    private startPromise: Promise<any> | null = null;
    constructor(app: App, private onScan: (text: string) => void) { super(app); }

    onOpen() {
        const { contentEl } = this;
        contentEl.createEl('h2', { text: 'Scan QR Code' });
        const readerId = 'od-qr-reader';
        contentEl.createDiv({ attr: { id: readerId } });

        // The scanner library is loaded on demand; onClose may run before it resolves,
        // so startPromise gates cleanup on the whole load-and-start sequence.
        this.startPromise = loadHtml5Qrcode().then(Html5Qrcode => {
            this.html5QrCode = new Html5Qrcode(readerId);
            return this.html5QrCode.start({ facingMode: "environment" }, { fps: 10, qrbox: { width: 250, height: 250 } }, (decodedText) => {
                this.onScan(decodedText);
                this.close();
            }, () => {});
        }).catch(err => {
            contentEl.createEl('p', { text: 'Error starting camera: ' + err, cls: 'mod-warning' });
        });
    }
    
    onClose() {
        const cleanup = () => {
            if (this.html5QrCode && this.html5QrCode.isScanning) {
                this.html5QrCode.stop().then(() => {
                    this.html5QrCode?.clear();
                    this.contentEl.empty();
                }).catch((err) => {
                    console.error(err);
                    this.contentEl.empty();
                });
            } else {
                this.contentEl.empty();
            }
        };
        if (this.startPromise) {
            this.startPromise.then(cleanup).catch(console.error);
        } else {
            cleanup();
        }
    }
}

export class SelectPeerModal extends Modal {
    constructor(app: App, private connections: Map<string, DataConnection>, private clusterPeers: Map<string, PeerInfo>, private onSubmit: (peerId: string) => void) { super(app); }
    
    onOpen() {
        const { contentEl } = this;
        contentEl.createEl('h2', { text: 'Force Full Sync with Peer' });
        contentEl.createEl('p', { text: 'This will perform a two-way sync. Files will be exchanged based on which is newer. This is the safest way to reconcile two vaults.' }).addClass('mod-warning');
        let selectedPeer = '';
        const peerList = Array.from(this.connections.keys());
        if (peerList.length === 0) {
            contentEl.createEl('p', { text: 'No peers are currently connected.' });
            new Setting(contentEl).addButton(btn => btn.setButtonText("OK").onClick(() => this.close()));
            return;
        }
        if (peerList.length > 0) {
            selectedPeer = peerList[0];
        }
        new Setting(contentEl).setName('Sync with Device').addDropdown(dropdown => {
            peerList.forEach(peerId => {
                const peerInfo = this.clusterPeers.get(peerId);
                dropdown.addOption(peerId, peerInfo?.friendlyName || peerId);
            });
            dropdown.setValue(selectedPeer);
            dropdown.onChange(value => selectedPeer = value);
        });
        new Setting(contentEl)
            .addButton(btn => btn.setButtonText('Cancel').onClick(() => this.close()))
            .addButton(btn => btn.setButtonText('Start Full Sync').setWarning().onClick(() => {
                if (selectedPeer) this.onSubmit(selectedPeer);
                this.close();
            }));
    }
    
    onClose() {
        this.contentEl.empty();
    }
}

export class ConflictCenter {
    private conflicts: Map<string, string> = new Map();
    private ribbonEl: HTMLElement | null = null;
    
    constructor(private app: App, private plugin: ObsidianDecentralizedPlugin) { }
    
    registerRibbon() {
        this.ribbonEl = this.plugin.addRibbonIcon('swords', 'Resolve Sync Conflicts', () => this.showConflictList());
        this.ribbonEl.style.display = 'none';
    }
    
    addConflict(originalPath: string, conflictPath: string) {
        this.conflicts.set(originalPath, conflictPath);
        this.updateRibbon();
    }
    
    resolveConflict(originalPath: string) {
        this.conflicts.delete(originalPath);
        this.updateRibbon();
    }
    
    updateRibbon() {
        if (!this.ribbonEl) return;
        if (this.conflicts.size > 0) {
            this.ribbonEl.show();
            this.ribbonEl.setAttribute('aria-label', `Resolve ${this.conflicts.size} sync conflicts`);
            let badge = this.ribbonEl.querySelector('.od-conflict-badge') as HTMLElement;
            if (!badge) {
                badge = document.createElement('span');
                badge.className = 'od-conflict-badge';
                this.ribbonEl.style.position = 'relative';
                this.ribbonEl.appendChild(badge);
            }
            badge.innerText = this.conflicts.size.toString();
        } else {
            this.ribbonEl.hide();
        }
    }
    
    showConflictList() {
        new ConflictListModal(this.app, this, this.plugin).open();
    }
}

export class ConflictListModal extends Modal {
    constructor(app: App, private conflictCenter: ConflictCenter, private plugin: ObsidianDecentralizedPlugin) { super(app); }
    
    onOpen() {
        const { contentEl } = this;
        contentEl.createEl('h2', { text: 'Sync Conflicts' });
        const conflicts: Map<string, string> = (this.conflictCenter as any).conflicts;
        if (conflicts.size === 0) {
            contentEl.createEl('p', { text: 'No conflicts to resolve.' });
            return;
        }
        conflicts.forEach((conflictPath, originalPath) => {
            new Setting(contentEl).setName(originalPath).setDesc(`Conflict file: ${conflictPath}`)
                .addButton(btn => btn.setButtonText('Resolve').setCta().onClick(async () => {
                    this.close();
                    await this.showResolutionModal(originalPath, conflictPath);
                }));
        });
    }
    
    async showResolutionModal(originalPath: string, conflictPath: string) {
        const originalFile = this.app.vault.getAbstractFileByPath(originalPath) as TFile;
        const conflictFile = this.app.vault.getAbstractFileByPath(conflictPath) as TFile;
        if (!originalFile || !conflictFile) {
                this.plugin.showNotice("One of the conflict files is missing.", 'error');
                this.conflictCenter.resolveConflict(originalPath);
                return;
            }
            try {
                if (this.plugin.isBinary(originalFile.extension)) {
                    new BinaryConflictResolutionModal(this.app, originalFile.name, async (choice) => {
                        this.plugin.ignoreNextEventForPath(originalPath);
                        if (choice === 'remote') {
                            const remoteData = await this.app.vault.readBinary(conflictFile);
                            await this.app.vault.modifyBinary(originalFile, remoteData);
                        }
                        this.plugin.ignoreNextEventForPath(conflictPath);
                        await this.app.vault.delete(conflictFile);
                        this.conflictCenter.resolveConflict(originalPath);
                        this.plugin.showNotice(`${originalPath} has been resolved.`, 'info');
                    }).open();
                } else {
                    const localContent = await this.app.vault.read(originalFile);
                    const remoteContent = await this.app.vault.read(conflictFile);
                    new ConflictResolutionModal(this.app, localContent, remoteContent, async (chosenContent) => {
                        this.plugin.ignoreNextEventForPath(originalPath);
                        await this.app.vault.modify(originalFile, chosenContent);
                        this.plugin.ignoreNextEventForPath(conflictPath);
                        await this.app.vault.delete(conflictFile);
                        this.conflictCenter.resolveConflict(originalPath);
                        this.plugin.showNotice(`${originalPath} has been resolved.`, 'info');
                    }).open();
                }
            } catch (e) {
                new Notice('Failed to read conflict files: ' + (e instanceof Error ? e.message : String(e)));
                this.conflictCenter.resolveConflict(originalPath);
            }
        }
    
    onClose() {
        this.contentEl.empty();
    }
}

export class ConflictResolutionModal extends Modal {
    constructor(app: App, private localContent: string, private remoteContent: string, private onResolve: (chosenContent: string) => void) { super(app); }
    
    onOpen() {
        const { contentEl } = this;
        contentEl.addClass('obsidian-decentralized-diff-modal');
        contentEl.createEl('h2', { text: 'Resolve Conflict' });
        const dmp = new DiffMatchPatch();
        const diff = dmp.diff_main(this.localContent, this.remoteContent);
        dmp.diff_cleanupSemantic(diff);
        const diffEl = contentEl.createDiv({ cls: 'obsidian-decentralized-diff-view' });
        for (const [op, text] of diff) {
            const span = diffEl.createSpan();
            span.setText(text);
            if (op === 1) span.addClass('od-diff-insert');
            if (op === -1) span.addClass('od-diff-delete');
        }
        new Setting(contentEl)
            .addButton(btn => btn.setButtonText('Keep My Version').onClick(() => {
                this.onResolve(this.localContent);
                this.close();
            }))
            .addButton(btn => btn.setButtonText('Use Their Version').setWarning().onClick(() => {
                this.onResolve(this.remoteContent);
                this.close();
            }));
    }
    
    onClose() {
        this.contentEl.empty();
    }
}

export class BinaryConflictResolutionModal extends Modal {
    constructor(app: App, private fileName: string, private onResolve: (choice: 'local' | 'remote') => void) { super(app); }
    
    onOpen() {
        const { contentEl } = this;
        contentEl.createEl('h2', { text: 'Resolve Binary Conflict' });
        contentEl.createEl('p', { text: `The file "${this.fileName}" is a binary file (e.g. image, pdf, audio). Differences cannot be shown.` });
        new Setting(contentEl).addButton(btn => btn.setButtonText('Keep My Version (Local)').onClick(() => {
            this.onResolve('local');
            this.close();
        })).addButton(btn => btn.setButtonText('Use Their Version (Remote)').setWarning().onClick(() => {
            this.onResolve('remote');
            this.close();
        }));
    }
    
    onClose() {
        this.contentEl.empty();
    }
}

/**
 * Confirmation prompt for actions that cannot be undone. The settings tab puts several of
 * these behind bare icon buttons, where a single stray click used to disconnect or forget a
 * device with no way back.
 */
export class ConfirmModal extends Modal {
    constructor(
        app: App,
        private opts: { title: string; body: string; confirmText: string; onConfirm: () => void }
    ) { super(app); }

    onOpen() {
        const { contentEl } = this;
        contentEl.createEl('h2', { text: this.opts.title });
        contentEl.createEl('p', { text: this.opts.body });
        new Setting(contentEl)
            .addButton(btn => btn.setButtonText('Cancel').onClick(() => this.close()))
            .addButton(btn => btn.setButtonText(this.opts.confirmText).setWarning().onClick(() => {
                this.close();
                this.opts.onConfirm();
            }));
    }

    onClose() {
        this.contentEl.empty();
    }
}

export class SyncProgressModal extends Modal {
    private container: HTMLElement;
    private refreshInterval: number | null = null;
    private lastSignature: string | null = null;

    constructor(app: App, private plugin: ObsidianDecentralizedPlugin) { super(app); }

    onOpen() {
        const { contentEl } = this;
        contentEl.addClass('od-progress-modal');
        contentEl.createEl('h2', { text: 'Sync Progress' });
        this.container = contentEl.createDiv();
        this.refresh();
        this.refreshInterval = window.setInterval(() => this.refresh(), 500);
    }

    onClose() {
        if (this.refreshInterval) clearInterval(this.refreshInterval);
        this.contentEl.empty();
    }

    /**
     * Cheap fingerprint of everything this modal displays.
     *
     * refresh() tears down and rebuilds the whole subtree, and it runs twice a second
     * for as long as the modal is open — including when absolutely nothing is happening.
     * The time bucket is only mixed in while work is in flight, so the elapsed-time and
     * throughput readouts still tick, but an idle modal settles to zero DOM work.
     */
    private stateSignature(): string {
        const s = this.plugin.syncState;
        const parts: string[] = [
            s.isSyncing ? `S${s.currentPhase}|${s.filesTransferred}/${s.filesTotal}|${s.bytesTransferred}/${s.bytesTotal}|${s.currentFile ?? ''}|${s.currentFileSize ?? ''}` : 'S-',
        ];
        for (const t of this.plugin.activeTransfers.values()) {
            parts.push(`T${t.id}:${t.processedChunks}/${t.totalChunks}:${t.status}:${t.direction}`);
        }
        for (const f of this.plugin.failedSyncs) {
            parts.push(`F${f.path}:${f.type}:${f.retryCount}:${f.timestamp}`);
        }
        const busy = s.isSyncing || this.plugin.activeTransfers.size > 0;
        if (busy) parts.push(`t${Math.floor(Date.now() / 1000)}`);
        return parts.join('\n');
    }

    refresh() {
        const signature = this.stateSignature();
        if (signature === this.lastSignature) return;
        this.lastSignature = signature;

        this.container.empty();
        const hasActive = this.plugin.activeTransfers.size > 0;
        const hasFailed = this.plugin.failedSyncs.length > 0;
        const isSyncing = this.plugin.syncState.isSyncing;

        if (!hasActive && !hasFailed && !isSyncing) {
            this.container.createEl('p', { text: 'No active file transfers or syncs.' });
            return;
        }

        if (isSyncing) {
            this.container.createEl('h4', { text: 'Full Sync Progress', attr: { style: 'margin-top: 0;' } });
            const item = this.container.createDiv({ cls: 'od-transfer-item' });
            item.createDiv({ text: describeSyncPhase(this.plugin.syncState.currentPhase), cls: 'od-phase-label' });
            
            if (this.plugin.syncState.filesTotal > 0) {
                item.createEl('progress', { attr: { value: this.plugin.syncState.filesTransferred, max: this.plugin.syncState.filesTotal } });
                const meta = item.createDiv({ cls: 'od-transfer-meta' });
                meta.createSpan({ text: `${this.plugin.syncState.filesTransferred} / ${this.plugin.syncState.filesTotal} files` });
                meta.createSpan({ text: `${formatBytes(this.plugin.syncState.bytesTransferred)} / ${formatBytes(this.plugin.syncState.bytesTotal)}` });
                
                const elapsedSeconds = (Date.now() - (this.plugin.syncState.syncStartTime || Date.now())) / 1000;
                const speedBytesPerSec = elapsedSeconds > 0 ? this.plugin.syncState.bytesTransferred / elapsedSeconds : 0;
                meta.createSpan({ text: `${formatBytes(speedBytesPerSec)}/s` });

                if (this.plugin.syncState.currentFile) {
                    const currentFileMeta = item.createDiv({ cls: 'od-transfer-meta', attr: { style: 'margin-top: 5px; color: var(--text-muted); font-size: 0.85em;' } });
                    currentFileMeta.createSpan({ text: `Syncing: ${this.plugin.syncState.currentFile}` });
                    if (this.plugin.syncState.currentFileSize != null) {
                        currentFileMeta.createSpan({ text: ` (${formatBytes(this.plugin.syncState.currentFileSize)})` });
                    }
                }
            } else {
                item.createDiv({ text: 'Analyzing differences...', cls: 'od-transfer-meta' });
            }
        }

        if (hasActive) {
            if (hasFailed || isSyncing) this.container.createEl('h4', { text: 'Active Data Transfers', attr: { style: 'margin-top: 20px;' } });
            this.plugin.activeTransfers.forEach(transfer => {
                const item = this.container.createDiv({ cls: 'od-transfer-item' });
                const title = item.createDiv({ cls: 'od-transfer-title', attr: { style: 'display: flex; align-items: center; margin-bottom: 4px;' } });
                
                const iconSpan = title.createSpan({ cls: 'od-transfer-icon' });
                setIcon(iconSpan, transfer.direction === 'upload' ? 'arrow-up-circle' : 'arrow-down-circle');
                iconSpan.style.marginRight = '6px';
                iconSpan.style.color = transfer.direction === 'upload' ? 'var(--text-success)' : 'var(--interactive-accent)';
                
                title.createSpan({ text: transfer.path, attr: { style: 'font-weight: 600;' } });
                if (transfer.status === 'paused') title.createSpan({ text: ' (Paused)', attr: { style: 'color: var(--text-muted); font-size: 0.8em; margin-left: 6px;' } });
                
                item.createEl('progress', { attr: { value: transfer.processedChunks, max: transfer.totalChunks } });

                const now = Date.now();
                const elapsedSeconds = (now - transfer.startTime) / 1000;
                const chunkSize = transfer.chunkSize || this.plugin.getChunkSize();
                const processedBytes = transfer.processedChunks * chunkSize;
                const totalBytes = transfer.totalChunks * chunkSize;
                const speedBytesPerSec = elapsedSeconds > 0 ? processedBytes / elapsedSeconds : 0;
                const remainingBytes = totalBytes - processedBytes;
                const remainingSeconds = speedBytesPerSec > 0 && Number.isFinite(speedBytesPerSec) ? remainingBytes / speedBytesPerSec : 0;

                const meta = item.createDiv({ cls: 'od-transfer-meta' });
                meta.createSpan({ text: `${formatBytes(speedBytesPerSec)}/s` });
                
                const remText = remainingSeconds > 0 && Number.isFinite(remainingSeconds) ? `${Math.round(remainingSeconds)}s remaining` : 'Unknown time remaining';
                meta.createSpan({ text: remText });
                
                const pct = transfer.totalChunks > 0 ? (transfer.processedChunks / transfer.totalChunks) * 100 : 0;
                meta.createSpan({ text: `${Math.round(pct)}%` });
            });
        }

        if (hasFailed) {
            this.container.createEl('h4', { text: 'Pending Retries', attr: { style: hasActive ? 'margin-top: 20px;' : 'margin-top: 0;' } });
            this.plugin.failedSyncs.forEach(fail => {
                const item = this.container.createDiv({ cls: 'od-transfer-item' });
                const title = item.createDiv({ cls: 'od-transfer-title', attr: { style: 'display: flex; align-items: center; margin-bottom: 4px;' } });
                
                const iconSpan = title.createSpan({ cls: 'od-transfer-icon' });
                setIcon(iconSpan, fail.type === 'file-delete' ? 'trash-2' : 'alert-circle');
                iconSpan.style.marginRight = '6px';
                iconSpan.style.color = 'var(--text-error)';
                title.createSpan({ text: fail.path, attr: { style: 'font-weight: 600;' } });

                const meta = item.createDiv({ cls: 'od-transfer-meta' });
                const peerName = fail.peerId ? (this.plugin.clusterPeers.get(fail.peerId)?.friendlyName || fail.peerId) : 'Broadcast';
                meta.createSpan({ text: `To: ${peerName}` });
                
                const secondsAgo = Math.round((Date.now() - fail.timestamp) / 1000);
                meta.createSpan({ text: `Failed ${secondsAgo}s ago (Attempt ${fail.retryCount || 0})` });

                if (fail.reason) {
                    const reasonMeta = item.createDiv({ cls: 'od-transfer-meta' });
                    reasonMeta.createSpan({ text: `Error: ${fail.reason}`, attr: { style: 'color: var(--text-error);' } });
                }
            });
        }
    }
}
