# AGY Relay ⚡

[![Version](https://img.shields.io/badge/version-v202608.0020-blue.svg)](https://github.com/Neidraidd/AGY-Relay)
[![Status](https://img.shields.io/badge/status-in--development-orange.svg)](https://github.com/Neidraidd/AGY-Relay)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

> **Fast, responsive web & mobile companion interface for Google Antigravity (AGY).**

**AGY Relay** transforms your command-line Antigravity coding assistant into a modern, real-time web application accessible across desktop browsers, tablets, and mobile devices over your local network.

---

## ✨ Features

- ⚡ **Real-Time Streaming** — Live token-by-token streaming powered by WebSockets and AGY's native structured JSON stream protocol.
- 📱 **Mobile-First PWA** — Built-in Progressive Web App support, touch swipe navigation gestures, and responsive drawer layout optimized for smartphones.
- 📁 **Unified Session Management** — Seamlessly resume previous CLI conversations or spawn dedicated new agent sessions.
- 📦 **Permanent Archive Storage** — One-click archive & restore system with persistent local JSON state retention across restarts.
- ⚙ **Customizable Tools Visibility** — Toggle internal tool calls (`run_command`, `grep`, `file_edit`) on or off with a single click.
- 🤖 **Model Selector** — Select or switch active Gemini models per session on the fly.
- 🔄 **Smart Auto-Reconnect** — Instant background keepalive and auto-reconnection when switching tabs or unlocking mobile devices.
- 🖼 **Image & File Attachments** — Drag-and-drop or upload images directly into conversations for multimodal agent queries.

---

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.10+
- [Google Antigravity (AGY)](https://github.com/google-deepmind) CLI installed and accessible in your `$PATH`.

### 2. Installation

Clone the repository:
```bash
git clone git@github.com:Neidraidd/AGY-Relay.git
cd AGY-Relay
```

Install Python dependencies:
```bash
pip install -r requirements.txt
```

### 3. Launching

Start the server:
```bash
./start.sh
```

Or run continuously with auto-restart daemon:
```bash
./daemon.sh
```

Open your browser at:
```
http://localhost:7788
```
*(Or your machine's local IP address, e.g. `http://192.168.x.x:7788`, to access from your mobile phone on the same Wi-Fi network).*

---

## ⚙ Configuration

You can customize runtime settings via environment variables:

| Variable | Default | Description |
|---|---|---|
| `AGY_PROXY_HOST` | `0.0.0.0` | IP interface to bind to |
| `AGY_PROXY_PORT` | `7788` | Port for the web interface |
| `AGY_WORKSPACE` | `$HOME` | Default working directory for the agent |
| `AGY_BIN` | `agy` | Path to the Antigravity CLI executable |

Example:
```bash
AGY_PROXY_PORT=8080 AGY_WORKSPACE=/home/user/projects ./start.sh
```

---

## 📱 Mobile PWA Installation

1. Open `http://<your-ip>:7788` in Chrome (Android) or Safari (iOS).
2. Tap **Settings / Share** -> **Add to Home Screen**.
3. Launch **AGY Relay** as a standalone, fullscreen native-like app!

---

## ⚠️ Security Warning & Privacy

> [!CAUTION]
> **DO NOT EXPOSE AGY RELAY TO THE PUBLIC INTERNET OR FORWARD PORT 7788 ON YOUR ROUTER.**

- **No Authentication / Encryption**: AGY Relay is designed strictly as an internal development companion for your **trusted local network / Wi-Fi only** (or secure VPN like Tailscale / WireGuard).
- **Direct Shell & Code Access**: Because AGY executes system commands and file edits on your host machine, exposing this proxy to the public internet without an authentication layer would allow anyone with your IP to execute arbitrary code on your computer.
- **Local Isolation**: All chats, history, and uploaded images stay strictly stored on your local disk. Nothing is transmitted to third-party telemetry servers.

---

## 📄 License

GNU General Public License v3.0 (GPLv3) © 2026 Neidraidd. See [LICENSE](LICENSE) for details.
