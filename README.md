# P2P File Transfer

Direct device-to-device large file transfer software built on WebRTC and Node.js. Enables transferring large files (4 GB, 5 GB, 20 GB+) directly between laptops and mobile devices without cloud storage limits or server fees.

## Features

- Direct Peer-to-Peer Transfer: Files stream directly browser-to-browser via WebRTC DataChannels.
- No File Size Limits: Uses 64 KB chunking and backpressure flow control to transfer files of any size without exhausting device memory.
- Zero Cloud Storage Costs: Files never upload to any central server. Only lightweight network signaling is relayed.
- Cross-Platform: Works across Chrome, Safari, Firefox, and Edge on Windows, macOS, Linux, Android, and iOS.
- Direct Pairing: Connect devices via 6-digit numeric codes or by scanning the high-contrast QR code.
- Clean Light UI: Minimalist, utilitarian interface with real-time transfer metrics.
- End-to-End Encrypted: Data packets are encrypted in transit via standard DTLS and SCTP protocols.

## Getting Started

### Prerequisites

- Node.js (v18 or higher)
- npm

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/AmirRazaRangrez/File-Share.git
   cd File-Share
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the server:
   ```bash
   npm start
   ```

4. Open the application:
   - On the host device: `http://localhost:5000`
   - On mobile devices on the same network: `http://<your-local-ip>:5000`

## Architecture

- Frontend: Vanilla JavaScript, HTML5, and CSS with WebRTC DataChannel APIs and File System Access streaming.
- Signaling Server: Lightweight Node.js server using the `ws` library to exchange SDP offers/answers and ICE candidates.
- NAT Traversal: Google public STUN servers (`stun:stun.l.google.com:19302`).

## License

MIT
