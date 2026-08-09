# Obsidian Decentralized

![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)
![Obsidian Decentralized Banner](https://mwassets.github.io/files/obsidian-decentralized/banner.png)

Sync your Obsidian notes across devices **without a central server**. This plugin uses peer-to-peer technologies (WebRTC via PeerJS) to transfer your files directly between your devices, offering a free, private, and local-first alternative to cloud-based sync services.

Your notes are your own. This plugin ensures they stay that way.

---

## 📋 Table of Contents

- [Core Features](#-core-features)
- [How It Works](#-how-it-works)
- [Manual Installation](#-manual-installation)
- [🚀 Getting Started: Connecting Your First Devices](#-getting-started-connecting-your-first-devices)
  - [Method A: LAN Discovery (Easiest, LAN Only)](#method-a-lan-discovery-easiest-lan-only)
  - [Method B: By ID / QR Code (Works over Internet)](#method-b-by-id--qr-code-works-over-internet)
  - [Method C: Companion Mode (For Convenience)](#method-c-companion-mode-for-convenience)
- [⚙️ Configuration & Advanced Features](#️-configuration--advanced-features)
  - [Selective Sync](#selective-sync)
  - [Conflict Resolution](#conflict-resolution)
  - [Syncing Attachments and Config Files](#syncing-attachments-and-config-files)
  - [Experimental: Direct IP Mode](#experimental-direct-ip-mode)
  - [Using a Custom Signaling Server](#using-a-custom-signaling-server)
- [Conflict Center](#-conflict-center)
- [Security and Privacy](#-security-and-privacy)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

---

## ✨ Core Features

-   **🌐 True Peer-to-Peer Sync:** Files are sent directly from one device to another. No cloud storage, no middleman.
-   **🕵️‍♂️ LAN Discovery:** Automatically find other devices on your local network without needing to copy/paste IDs.
-   **🤝 Multiple Connection Methods:** Connect via auto-discovery with a PIN, a unique device ID, a QR code, or (experimentally) a direct IP address.
-   **⚙️ Powerful Sync Engine:**
    -   Handles file/folder creation, deletion, and renaming.
    -   Efficiently syncs only the changes.
    -   Intelligently chunks large files to handle attachments and media.
-   **⚔️ Conflict Management:**
    -   Choose your preferred conflict resolution strategy: create a duplicate file (safest), last-write-wins, or attempt to auto-merge changes in Markdown files.
    -   A dedicated "Conflict Center" in the ribbon helps you review and resolve conflicts.
-   **🎛️ Granular Control:**
    -   Selectively include or exclude folders from sync.
    -   Optionally sync all file types (images, PDFs, etc.).
    -   Optionally sync your `.obsidian` config folder (use with caution!).
-   **🏡 Fully Self-Hostable:** For ultimate privacy, you can run your own PeerJS signaling server.
-   **📱 Cross-Platform:** Works on Desktop (Windows, macOS, Linux) and Mobile (via PeerJS). LAN Discovery is desktop-only.

## 🤔 How It Works

This plugin uses **PeerJS**, which leverages **WebRTC** technology. Think of it like this:

1.  **The Matchmaker (Signaling Server):** When you want to connect two devices, they both check in with a public "signaling server." This server is like a switchboard operator; it introduces your devices to each other and helps them establish a direct communication channel.
2.  **The Direct Line (P2P Connection):** Once the introduction is made, the signaling server steps away. Your devices then communicate **directly** with each other.
3.  **Data Transfer:** All your files, changes, and deletions are sent over this direct, encrypted channel. **Your notes are never stored on any third-party server.** See [Security and Privacy](#-security-and-privacy) for exactly what "encrypted" covers.

For LAN connections, the plugin can also use multicast UDP packets to broadcast its presence, allowing for automatic discovery without relying on an internet-based signaling server.

## 📥 Manual Installation

1.  Go to the [**Releases**](https://github.com/iWebbIO/obsidian-decentralized/releases) page on GitHub.
2.  Download **`main.js`**, **`manifest.json`**, and **`styles.css`** from the latest release. All three are required — without `styles.css` the pairing screens render unstyled.
3.  Navigate to your Obsidian vault's plugins folder, typically `<YourVault>/.obsidian/plugins/`.
    -   If you don't see a `.obsidian` folder, enable "Show hidden files" in your file explorer.
4.  Create a new folder inside `plugins` named `obsidian-decentralized`.
5.  Copy all three downloaded files into it.
6.  Restart Obsidian, or go to `Settings` → `Community Plugins` and toggle another plugin off and on.
7.  Go to `Settings` → `Community Plugins`. "Obsidian Decentralized" should now be listed.
8.  Click the toggle to **enable** the plugin.

## 🚀 Getting Started: Connecting Your First Devices

First, give your device a memorable name in the plugin settings (e.g., "My Desktop," "My Phone"). This makes it easier to identify.

Then open the connection helper: the **Connect Devices** button in settings, the **`users`** ribbon icon, or the "Connect to a device" command.

The helper has two tabs — **Quick Pair** and **Advanced**.

### Method A: Quick Pair (recommended)

Quick Pair shows your device's pairing code and a QR code, and lists other devices it finds on your Wi-Fi network.

**On Device A (e.g. your desktop):**
1.  Open the connection helper and stay on the **Quick Pair** tab.
2.  Either leave the QR code on screen, or press **Copy** to copy the pairing code.

**On Device B (e.g. your phone):**
1.  Open the connection helper on the **Quick Pair** tab.
2.  Either scan Device A's QR code with **Scan QR Code**, or paste the copied code into the input and press **Connect**.
3.  Alternatively, if both devices are on the same Wi-Fi, Device A should appear under "Or connect to nearby devices" — tap it.

> **🔑 Use the copied code, not the short device ID.** The copied code carries the encryption key as well as the device ID, and it is what turns on end-to-end encryption for the link. Treat it as a secret — anyone who has it can pair with you. Connecting by bare device ID (including by tapping a nearby device) pairs without a key.

### Method B: Primary sync partner (for automatic reconnection)

Once two devices have paired, you can mark one as the primary partner so they reconnect automatically.

1.  Open `Settings` → `Obsidian Decentralized` and find the device under **Current Cluster**.
2.  Click the **star** icon ("Set as Primary Sync Partner").

### Method C: Offline Mode (no internet at all)

See [Offline Mode](#offline-mode-no-signaling-server) below.

> **💡 Pro tip:** If two vaults ever look out of step, run a **Force Full Sync** — the `refresh-cw` ribbon icon, or the "Force full sync with a device" command — and pick the device to sync with.

## ⚙️ Configuration & Advanced Features

All options live in the plugin's settings tab (`Settings` → `Obsidian Decentralized`). The **Mode** dropdown at the top controls how much is shown: `Auto` keeps things minimal with safe defaults, `Manual` exposes the common settings, and `Advanced` adds tuning and security options.

### Selective Sync

-   **Included folders:** Only sync folders that are in this list (one path per line). If this is empty, all folders are synced by default.
-   **Excluded folders:** Never sync folders in this list. This takes priority over the included list.

### Conflict Resolution

When a file is changed on two devices before they have a chance to sync, a conflict occurs. Choose how you want to handle this:

-   **Create Conflict File (default and safest):** The incoming change is saved as a new file, e.g. `My Note (conflict on 2023-10-27).md`, so you can compare and merge them yourself.
-   **Last Write Wins:** The version with the newest modification time is kept and the other is discarded.

When exactly two devices are paired and two-device optimizations are on, the plugin instead resolves conflicts by role: the primary device's copy wins. This is automatic and overrides the setting above.

### Syncing Attachments and Config Files

-   **Sync all file types:** By default, the plugin focuses on text files. Enable this to sync images, PDFs, audio, and other attachments.
-   **Sync '.obsidian' configuration folder:** **(DANGEROUS)** Syncs your Obsidian settings, themes, and snippets. This can cause problems if your devices have different plugins, themes, or operating systems. **Always make a backup before enabling this.**

### Offline Mode (no signaling server)

<a id="offline-mode-no-signaling-server"></a>

For LAN-only environments with no internet, or where you don't want a signaling server involved at all. One device (usually a desktop) hosts and the others connect to it.

**On the host (desktop only):**
1.  Open the connection helper → **Advanced** tab → **Switch to Offline Mode**.
2.  Press **Start Hosting**. The screen shows the host's IP address and a security token, with a button to copy the token.

**On each other device:**
1.  Open the connection helper → **Advanced** tab → **Switch to Offline Mode**.
2.  Under **Join a Network**, enter the host's IP address and token, then connect. Hosts found on your Wi-Fi are also listed and can be selected directly.

Unlike the default mode, Offline Mode authenticates the connection: the host rejects any client presenting the wrong token.

### Using a Custom Signaling Server

For maximum privacy, you can run your own [PeerServer](https://github.com/peers/peerjs-server). In the plugin's "Advanced Settings," enable "Use custom signaling server" and enter your server's details.

## ⚔️ Conflict Center

If a conflict occurs and a `(conflict)` file is created, a new icon (`swords`) will appear in the left ribbon. This is the Conflict Center.

-   The icon shows a badge with the number of unresolved conflicts.
-   Clicking it opens a modal listing all conflicts.
-   Click `Resolve` on any conflict to open a diff view, allowing you to compare your local version with the remote version and choose which one to keep.

## 🛡️ Security and Privacy

-   **No cloud storage.** Your notes are never stored on a third-party server. They exist only on your devices.
-   **Transport encryption, always.** Every WebRTC connection is encrypted in transit with DTLS. This protects the data on the wire but says nothing about *who* is on the other end.
-   **Application-layer encryption, when you pair with a key.** Pairing with the full copied code (or by QR) also exchanges a 256-bit AES-GCM key, and traffic on that link is encrypted with it on top of DTLS. Pairing by bare device ID does not exchange a key, so that link has no application-layer encryption. There is currently no indicator in the UI distinguishing the two — check that you pasted the copied code, not just the device ID.
-   **The signaling server sees metadata, not notes.** In the default mode your devices register a stable ID with a public PeerJS server so they can find each other. It never handles note content, but it does see your device ID and IP address each session. Run your own PeerServer, or use Offline Mode, to avoid it entirely.
-   **Know the limits.** By default, a device that knows your device ID can open a connection to you. The "strict security" setting under Advanced hardens this by refusing unrecognised and unencrypted peers; it is off by default because turning it on requires re-pairing existing devices. Offline Mode is token-authenticated regardless.

## ⚠️ Troubleshooting

-   **Plugin doesn't appear in Obsidian:** Check the folder structure is `<YourVault>/.obsidian/plugins/obsidian-decentralized/` and that this folder directly contains `main.js`, `manifest.json`, and `styles.css`.
-   **Connection Fails:**
    -   Ensure both devices are connected to the internet (for the default PeerJS mode).
    -   Check for firewalls or aggressive ad-blockers (like Pi-hole) that might be blocking the connection to the PeerJS signaling server or the P2P connection itself.
    -   Double-check that you have copied/pasted the Peer ID correctly.
-   **Status is "❌ Sync Offline":** The plugin couldn't connect to the signaling server. It will automatically keep retrying with an increasing backoff delay. Check your internet connection.
-   **LAN Discovery Doesn't Work:** This feature requires UDP multicast, which is sometimes blocked by corporate networks, VPNs, or strict firewall rules. In this case, fall back to connecting via ID/QR Code.

## 🤝 Contributing

Contributions, bug reports, and feature requests are welcome! Please feel free to open an issue or submit a pull request.

## 📜 License

This plugin is licensed under the **GNU General Public License v3.0**. For the full license text, please see the `LICENSE` file included in the repository.

Special thanks to [Ray Vermey](https://github.com/rayvermey) for their guidance, encouragement and feedback.
