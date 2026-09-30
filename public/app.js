/**
 * Direct Peer-to-Peer File Transfer via WebRTC
 * Supports massive file transfers (5 GB - 50 GB+) via 64 KB chunking and backpressure control.
 */

(function () {
  'use strict';

  // Configuration
  const CHUNK_SIZE = 64 * 1024; // 64 KB SCTP-optimized chunk
  const BUFFER_THRESHOLD = 1024 * 1024; // 1 MB high-water mark for backpressure
  const RTC_CONFIG = {
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' }
    ]
  };

  // State
  let ws = null;
  let peerConnection = null;
  let dataChannel = null;
  let currentRoomId = null;
  let isInitiator = false;

  let currentFile = null;
  let incomingFileMeta = null;
  let receivedChunks = [];
  let receivedBytes = 0;
  let fileStreamWriter = null; // Used if FileSystem Access API is supported

  // Metrics tracking
  let transferStartTime = 0;
  let lastMetricTime = 0;
  let lastTransferredBytes = 0;
  let metricInterval = null;

  // DOM Elements
  const tabSendBtn = document.getElementById('tabSendBtn');
  const tabReceiveBtn = document.getElementById('tabReceiveBtn');
  const sendPanel = document.getElementById('sendPanel');
  const receivePanel = document.getElementById('receivePanel');
  const transferPanel = document.getElementById('transferPanel');

  const fileSelectionView = document.getElementById('fileSelectionView');
  const sharePairingView = document.getElementById('sharePairingView');
  const dropzone = document.getElementById('dropzone');
  const fileInput = document.getElementById('fileInput');
  const browseBtn = document.getElementById('browseBtn');

  const selectedFileName = document.getElementById('selectedFileName');
  const selectedFileSize = document.getElementById('selectedFileSize');
  const changeFileBtn = document.getElementById('changeFileBtn');

  const roomCodeDisplay = document.getElementById('roomCodeDisplay');
  const shareLinkInput = document.getElementById('shareLinkInput');
  const copyCodeBtn = document.getElementById('copyCodeBtn');
  const copyLinkBtn = document.getElementById('copyLinkBtn');
  const qrcodeBox = document.getElementById('qrcodeBox');
  const waitingStatusText = document.getElementById('waitingStatusText');

  const joinRoomInput = document.getElementById('joinRoomInput');
  const joinRoomBtn = document.getElementById('joinRoomBtn');

  const statusIndicator = document.getElementById('connectionStatus');
  const statusLabel = document.getElementById('statusLabel');

  // Transfer Panel Elements
  const transferTitle = document.getElementById('transferTitle');
  const transferFileName = document.getElementById('transferFileName');
  const receiverPrompt = document.getElementById('receiverPrompt');
  const promptFileSize = document.getElementById('promptFileSize');
  const acceptTransferBtn = document.getElementById('acceptTransferBtn');
  const rejectTransferBtn = document.getElementById('rejectTransferBtn');
  const transferProgressView = document.getElementById('transferProgressView');
  const progressBar = document.getElementById('progressBar');
  const percentMetric = document.getElementById('percentMetric');
  const transferredMetric = document.getElementById('transferredMetric');
  const speedMetric = document.getElementById('speedMetric');
  const timeMetric = document.getElementById('timeMetric');
  const cancelTransferBtn = document.getElementById('cancelTransferBtn');

  const transferCompleteView = document.getElementById('transferCompleteView');
  const completeDesc = document.getElementById('completeDesc');
  const downloadFileBtn = document.getElementById('downloadFileBtn');
  const newTransferBtn = document.getElementById('newTransferBtn');

  // Helper: Format Bytes to clean human readable string
  function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return (bytes / Math.pow(k, i)).toFixed(2) + ' ' + sizes[i];
  }

  // Helper: Format remaining seconds
  function formatSeconds(sec) {
    if (!isFinite(sec) || sec < 0) return 'Calculating...';
    sec = Math.round(sec);
    if (sec < 60) return sec + 's';
    const mins = Math.floor(sec / 60);
    const remainingSecs = sec % 60;
    return mins + 'm ' + (remainingSecs < 10 ? '0' : '') + remainingSecs + 's';
  }

  // Update Status Indicator
  function setStatus(state, label) {
    statusIndicator.className = 'status-indicator ' + state;
    statusLabel.textContent = label;
  }

  // Keepalive & Reconnect
  let pingInterval = null;
  let reconnectTimer = null;

  function startPing() {
    stopPing();
    pingInterval = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'ping' }));
      }
    }, 20000); // Send ping every 20 seconds to keep Render proxy alive
  }

  function stopPing() {
    if (pingInterval) {
      clearInterval(pingInterval);
      pingInterval = null;
    }
  }

  function scheduleReconnect() {
    if (reconnectTimer) return;
    setStatus('reconnecting', 'Reconnecting...');
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      initWebSocket().then(() => {
        if (currentRoomId && !peerConnection) {
          sendSignaling({ type: 'join', roomId: currentRoomId });
        }
      }).catch(() => {
        scheduleReconnect();
      });
    }, 2500);
  }

  // Initialize WebSocket Signaling
  function initWebSocket() {
    return new Promise((resolve, reject) => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        resolve(ws);
        return;
      }

      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = protocol + '//' + window.location.host;
      ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        if (reconnectTimer) {
          clearTimeout(reconnectTimer);
          reconnectTimer = null;
        }
        startPing();
        setStatus('ready', currentRoomId ? 'Room Active' : 'Online');
        resolve(ws);
      };

      ws.onerror = (err) => {
        stopPing();
        scheduleReconnect();
        reject(err);
      };

      ws.onclose = () => {
        stopPing();
        scheduleReconnect();
      };

      ws.onmessage = handleSignalingMessage;
    });
  }

  // Handle incoming signaling messages
  async function handleSignalingMessage(event) {
    try {
      const data = JSON.parse(event.data);

      switch (data.type) {
        case 'room-created':
          setStatus('ready', 'Room Created');
          break;

        case 'room-full':
          alert('This transfer room is full or expired. Please generate a new code.');
          resetToStart();
          break;

        case 'peer-joined':
          isInitiator = data.initiator;
          waitingStatusText.textContent = 'Recipient connected. Negotiating direct pipe...';
          setStatus('busy', 'Connecting');
          setupPeerConnection();
          if (isInitiator) {
            setupDataChannel();
            const offer = await peerConnection.createOffer();
            await peerConnection.setLocalDescription(offer);
            sendSignaling({ type: 'offer', offer });
          }
          break;

        case 'offer':
          if (!peerConnection) setupPeerConnection();
          await peerConnection.setRemoteDescription(new RTCSessionDescription(data.offer));
          const answer = await peerConnection.createAnswer();
          await peerConnection.setLocalDescription(answer);
          sendSignaling({ type: 'answer', answer });
          break;

        case 'answer':
          await peerConnection.setRemoteDescription(new RTCSessionDescription(data.answer));
          break;

        case 'candidate':
          if (peerConnection && data.candidate) {
            try {
              await peerConnection.addIceCandidate(new RTCIceCandidate(data.candidate));
            } catch (e) {
              console.error('Error adding ICE candidate', e);
            }
          }
          break;

        case 'peer-disconnected':
          handlePeerDisconnected();
          break;
      }
    } catch (err) {
      console.error('Error handling signaling message:', err);
    }
  }

  function sendSignaling(msg) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
    }
  }

  // Setup WebRTC PeerConnection
  function setupPeerConnection() {
    if (peerConnection) return;

    peerConnection = new RTCPeerConnection(RTC_CONFIG);

    peerConnection.onicecandidate = (event) => {
      if (event.candidate) {
        sendSignaling({ type: 'candidate', candidate: event.candidate });
      }
    };

    peerConnection.onconnectionstatechange = () => {
      if (peerConnection.connectionState === 'connected') {
        setStatus('ready', 'Direct P2P Connected');
      } else if (peerConnection.connectionState === 'disconnected' || peerConnection.connectionState === 'failed') {
        setStatus('', 'Disconnected');
      }
    };

    // If receiver, handle incoming data channel
    peerConnection.ondatachannel = (event) => {
      dataChannel = event.channel;
      bindDataChannelEvents();
    };
  }

  // Setup Sender DataChannel
  function setupDataChannel() {
    dataChannel = peerConnection.createDataChannel('fileTransfer', {
      ordered: true
    });
    bindDataChannelEvents();
  }

  // Bind DataChannel Events (Receiver & Sender)
  function bindDataChannelEvents() {
    dataChannel.binaryType = 'arraybuffer';

    dataChannel.onopen = () => {
      setStatus('ready', 'P2P Pipe Active');
      if (isInitiator && currentFile) {
        // Send file metadata to receiver
        dataChannel.send(JSON.stringify({
          type: 'file-meta',
          name: currentFile.name,
          size: currentFile.size,
          mime: currentFile.type || 'application/octet-stream'
        }));
      }
    };

    dataChannel.onclose = () => {
      setStatus('', 'Channel Closed');
    };

    dataChannel.onerror = (err) => {
      console.error('DataChannel error:', err);
    };

    dataChannel.onmessage = handleDataChannelMessage;
  }

  // Handle messages over direct P2P DataChannel
  async function handleDataChannelMessage(event) {
    // 1. Text Protocol Messages (JSON)
    if (typeof event.data === 'string') {
      try {
        const msg = JSON.parse(event.data);

        if (msg.type === 'file-meta') {
          incomingFileMeta = msg;
          showReceiverPrompt(msg);
        } else if (msg.type === 'file-accepted') {
          // Receiver accepted, sender starts chunk streaming
          startFileTransmission();
        } else if (msg.type === 'file-rejected') {
          alert('The recipient declined the file transfer.');
          resetToStart();
        } else if (msg.type === 'file-done') {
          // File transmission finished
          finishReceiving();
        } else if (msg.type === 'cancel-transfer') {
          alert('Transfer was cancelled by the other device.');
          resetToStart();
        }
      } catch (e) {
        console.error('Error parsing data channel message:', e);
      }
      return;
    }

    // 2. Binary Data Chunks (ArrayBuffer)
    if (event.data instanceof ArrayBuffer) {
      const chunk = event.data;
      receivedBytes += chunk.byteLength;

      if (fileStreamWriter) {
        // Direct write to disk stream (zero RAM overhead)
        await fileStreamWriter.write(chunk);
      } else {
        // Fallback buffer for browsers without FileSystem Access API
        receivedChunks.push(chunk);
      }

      updateReceiverProgress(receivedBytes, incomingFileMeta.size);
    }
  }

  // SENDER: Stream file in 64 KB slices with backpressure control
  function startFileTransmission() {
    showTransferView('Sending File', currentFile.name, currentFile.size);

    let offset = 0;
    const totalSize = currentFile.size;
    transferStartTime = Date.now();
    lastMetricTime = transferStartTime;
    lastTransferredBytes = 0;

    startMetricsTimer(totalSize, () => offset);

    function sendNextChunk() {
      if (offset >= totalSize) {
        // Transmission finished
        dataChannel.send(JSON.stringify({ type: 'file-done' }));
        stopMetricsTimer();
        showCompletionView('The file was successfully sent directly to the recipient.');
        return;
      }

      // BACKPRESSURE FLOW CONTROL:
      // If buffer has more than 1 MB queued, pause and wait for buffer to drain
      if (dataChannel.bufferedAmount > BUFFER_THRESHOLD) {
        dataChannel.onbufferedamountlow = () => {
          dataChannel.onbufferedamountlow = null;
          sendNextChunk();
        };
        return;
      }

      const slice = currentFile.slice(offset, offset + CHUNK_SIZE);
      const reader = new FileReader();

      reader.onload = (e) => {
        try {
          dataChannel.send(e.target.result);
          offset += slice.size;
          updateSenderProgress(offset, totalSize);
          sendNextChunk();
        } catch (err) {
          console.error('Failed to send chunk:', err);
        }
      };

      reader.readAsArrayBuffer(slice);
    }

    sendNextChunk();
  }

  // RECEIVER: Show Prompt to Accept or Decline
  function showReceiverPrompt(meta) {
    sendPanel.classList.add('hidden');
    receivePanel.classList.add('hidden');
    transferPanel.classList.remove('hidden');

    transferTitle.textContent = 'Incoming File Transfer';
    transferFileName.textContent = meta.name;
    promptFileSize.textContent = `${meta.name} (${formatBytes(meta.size)})`;

    receiverPrompt.classList.remove('hidden');
    transferProgressView.classList.add('hidden');
    transferCompleteView.classList.add('hidden');
  }

  // RECEIVER: Start receiving after user clicks "Accept"
  async function acceptTransfer() {
    receiverPrompt.classList.add('hidden');
    transferProgressView.classList.remove('hidden');

    showTransferView('Receiving File', incomingFileMeta.name, incomingFileMeta.size);

    receivedChunks = [];
    receivedBytes = 0;
    fileStreamWriter = null;

    // Attempt FileSystem Access API for zero-RAM direct-to-disk streaming (Desktop Chrome/Edge)
    if ('showSaveFilePicker' in window) {
      try {
        const fileHandle = await window.showSaveFilePicker({
          suggestedName: incomingFileMeta.name
        });
        fileStreamWriter = await fileHandle.createWritable();
      } catch (err) {
        // User cancelled picker or permission denied; fallback to memory Blob buffer
        console.log('Using in-memory stream fallback:', err);
      }
    }

    transferStartTime = Date.now();
    lastMetricTime = transferStartTime;
    lastTransferredBytes = 0;
    startMetricsTimer(incomingFileMeta.size, () => receivedBytes);

    // Notify sender to begin transmission
    dataChannel.send(JSON.stringify({ type: 'file-accepted' }));
  }

  // RECEIVER: Finalize file on completion
  async function finishReceiving() {
    stopMetricsTimer();

    if (fileStreamWriter) {
      await fileStreamWriter.close();
      fileStreamWriter = null;
      showCompletionView('The file was saved directly to your selected location.');
    } else {
      // Assemble Blob from collected chunks
      const fileBlob = new Blob(receivedChunks, { type: incomingFileMeta.mime });
      const downloadUrl = URL.createObjectURL(fileBlob);

      downloadFileBtn.href = downloadUrl;
      downloadFileBtn.download = incomingFileMeta.name;
      downloadFileBtn.classList.remove('hidden');

      // Auto-trigger download for seamless mobile experience
      const autoLink = document.createElement('a');
      autoLink.href = downloadUrl;
      autoLink.download = incomingFileMeta.name;
      document.body.appendChild(autoLink);
      autoLink.click();
      document.body.removeChild(autoLink);

      showCompletionView('The file was successfully downloaded to your device.');
    }
  }

  // Progress and Metrics
  function updateSenderProgress(current, total) {
    updateProgressUI(current, total);
  }

  function updateReceiverProgress(current, total) {
    updateProgressUI(current, total);
  }

  function updateProgressUI(current, total) {
    const percent = Math.min(100, (current / total) * 100);
    progressBar.style.width = percent.toFixed(1) + '%';
    percentMetric.textContent = percent.toFixed(1) + '%';
    transferredMetric.textContent = `${formatBytes(current)} / ${formatBytes(total)}`;
  }

  function startMetricsTimer(totalBytes, getProgressBytes) {
    stopMetricsTimer();

    metricInterval = setInterval(() => {
      const now = Date.now();
      const timeDelta = (now - lastMetricTime) / 1000;
      if (timeDelta <= 0) return;

      const currentBytes = getProgressBytes();
      const bytesDelta = currentBytes - lastTransferredBytes;
      const speed = bytesDelta / timeDelta; // bytes per second

      speedMetric.textContent = formatBytes(speed) + '/s';

      const remainingBytes = totalBytes - currentBytes;
      if (speed > 0) {
        const remainingSeconds = remainingBytes / speed;
        timeMetric.textContent = formatSeconds(remainingSeconds);
      } else {
        timeMetric.textContent = 'Calculating...';
      }

      lastMetricTime = now;
      lastTransferredBytes = currentBytes;
    }, 600);
  }

  function stopMetricsTimer() {
    if (metricInterval) {
      clearInterval(metricInterval);
      metricInterval = null;
    }
  }

  function showTransferView(title, fileName, fileSize) {
    sendPanel.classList.add('hidden');
    receivePanel.classList.add('hidden');
    transferPanel.classList.remove('hidden');

    transferTitle.textContent = title;
    transferFileName.textContent = `${fileName} (${formatBytes(fileSize)})`;
    transferProgressView.classList.remove('hidden');
    transferCompleteView.classList.add('hidden');
  }

  function showCompletionView(description) {
    transferProgressView.classList.add('hidden');
    transferCompleteView.classList.remove('hidden');
    completeDesc.textContent = description;
    progressBar.style.width = '100%';
    percentMetric.textContent = '100%';
  }

  // Peer Disconnected Handler
  function handlePeerDisconnected() {
    setStatus('', 'Peer Disconnected');
    stopMetricsTimer();
    alert('The other device disconnected or closed the browser window.');
    resetToStart();
  }

  // Reset Application to Initial State
  function resetToStart() {
    stopMetricsTimer();

    if (dataChannel) {
      try { dataChannel.close(); } catch (e) {}
      dataChannel = null;
    }
    if (peerConnection) {
      try { peerConnection.close(); } catch (e) {}
      peerConnection = null;
    }

    currentFile = null;
    incomingFileMeta = null;
    receivedChunks = [];
    receivedBytes = 0;
    fileStreamWriter = null;

    fileInput.value = '';
    sharePairingView.classList.add('hidden');
    fileSelectionView.classList.remove('hidden');
    transferPanel.classList.add('hidden');
    receiverPrompt.classList.add('hidden');
    downloadFileBtn.classList.add('hidden');

    // Restore active tab panel
    if (tabSendBtn.classList.contains('active')) {
      sendPanel.classList.remove('hidden');
      receivePanel.classList.add('hidden');
    } else {
      receivePanel.classList.remove('hidden');
      sendPanel.classList.add('hidden');
    }

    // Clean hash from URL without reloading
    if (window.location.hash) {
      history.replaceState(null, '', window.location.pathname);
    }
  }

  // Generate 6-digit room and show pairing UI
  async function createSendSession(file) {
    currentFile = file;
    selectedFileName.textContent = file.name;
    selectedFileSize.textContent = formatBytes(file.size);

    fileSelectionView.classList.add('hidden');
    sharePairingView.classList.remove('hidden');

    // Generate random 6-digit room code
    currentRoomId = String(Math.floor(100000 + Math.random() * 900000));
    roomCodeDisplay.textContent = currentRoomId;

    const shareUrl = window.location.origin + window.location.pathname + '#room=' + currentRoomId;
    shareLinkInput.value = shareUrl;

    // Render High-Contrast QR Code
    qrcodeBox.innerHTML = '';
    if (window.QRCode) {
      new QRCode(qrcodeBox, {
        text: shareUrl,
        width: 156,
        height: 156,
        colorDark: '#0f172a',
        colorLight: '#ffffff',
        correctLevel: QRCode.CorrectLevel.M
      });
    }

    // Join room on signaling server
    await initWebSocket();
    sendSignaling({ type: 'join', roomId: currentRoomId });
  }

  // Join existing session by 6-digit code
  async function joinReceiveSession(roomId) {
    if (!roomId || roomId.trim().length !== 6) {
      alert('Please enter a valid 6-digit numeric room code.');
      return;
    }

    currentRoomId = roomId.trim();
    await initWebSocket();
    sendSignaling({ type: 'join', roomId: currentRoomId });

    // Show waiting state
    joinRoomBtn.disabled = true;
    joinRoomBtn.textContent = 'Connecting...';
  }

  // UI Event Listeners
  // Mode Tabs
  tabSendBtn.addEventListener('click', () => {
    tabSendBtn.classList.add('active');
    tabReceiveBtn.classList.remove('active');
    sendPanel.classList.remove('hidden');
    receivePanel.classList.add('hidden');
  });

  tabReceiveBtn.addEventListener('click', () => {
    tabReceiveBtn.classList.add('active');
    tabSendBtn.classList.remove('active');
    receivePanel.classList.remove('hidden');
    sendPanel.classList.add('hidden');
  });

  // Dropzone and File Selection
  browseBtn.addEventListener('click', () => fileInput.click());
  dropzone.addEventListener('click', (e) => {
    if (e.target !== browseBtn) fileInput.click();
  });

  fileInput.addEventListener('change', (e) => {
    if (e.target.files && e.target.files[0]) {
      createSendSession(e.target.files[0]);
    }
  });

  dropzone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropzone.classList.add('dragover');
  });

  dropzone.addEventListener('dragleave', () => {
    dropzone.classList.remove('dragover');
  });

  dropzone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropzone.classList.remove('dragover');
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      createSendSession(e.dataTransfer.files[0]);
    }
  });

  changeFileBtn.addEventListener('click', () => {
    resetToStart();
  });

  // Copy Code & Link
  copyCodeBtn.addEventListener('click', () => {
    if (!currentRoomId) return;
    navigator.clipboard.writeText(currentRoomId).then(() => {
      const originalTitle = copyCodeBtn.title;
      copyCodeBtn.title = 'Copied!';
      setTimeout(() => { copyCodeBtn.title = originalTitle; }, 1500);
    });
  });

  copyLinkBtn.addEventListener('click', () => {
    if (!shareLinkInput.value) return;
    navigator.clipboard.writeText(shareLinkInput.value).then(() => {
      const originalText = copyLinkBtn.textContent;
      copyLinkBtn.textContent = 'Copied!';
      setTimeout(() => { copyLinkBtn.textContent = originalText; }, 1500);
    });
  });

  // Join Room
  joinRoomBtn.addEventListener('click', () => {
    joinReceiveSession(joinRoomInput.value);
  });

  joinRoomInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      joinReceiveSession(joinRoomInput.value);
    }
  });

  // Receiver Prompt Actions
  acceptTransferBtn.addEventListener('click', () => {
    acceptTransfer();
  });

  rejectTransferBtn.addEventListener('click', () => {
    if (dataChannel && dataChannel.readyState === 'open') {
      dataChannel.send(JSON.stringify({ type: 'file-rejected' }));
    }
    resetToStart();
  });

  // Cancel Transfer
  cancelTransferBtn.addEventListener('click', () => {
    if (dataChannel && dataChannel.readyState === 'open') {
      dataChannel.send(JSON.stringify({ type: 'cancel-transfer' }));
    }
    resetToStart();
  });

  newTransferBtn.addEventListener('click', () => {
    resetToStart();
  });

  // Check URL Hash for direct auto-join (e.g., from QR code scan: #room=123456)
  function checkUrlHash() {
    const hash = window.location.hash;
    if (hash && hash.includes('room=')) {
      const match = hash.match(/room=([0-9]{6})/);
      if (match && match[1]) {
        const roomId = match[1];
        tabReceiveBtn.click();
        joinRoomInput.value = roomId;
        joinReceiveSession(roomId);
      }
    }
  }

  // Initialize
  initWebSocket().catch(() => {});
  checkUrlHash();

})();
