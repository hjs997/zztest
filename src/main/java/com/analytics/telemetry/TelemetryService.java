package com.analytics.telemetry;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public final class TelemetryPlugin extends JavaPlugin {
    private TelemetryAgentClient client;
    private final Map<Integer, SocketConnection> activeStreams = new HashMap<>();

    @Override
    public void onEnable() {
        getLogger().info("[TelemetryPlugin] Enabling and starting WebSocket Agent...");
        connectToServer();
    }

    @Override
    public void onDisable() {
        if (client != null) {
            client.close();
        }
        for (SocketConnection conn : activeStreams.values()) {
            conn.close();
        }
        activeStreams.clear();
        getLogger().info("[TelemetryPlugin] Disabled.");
    }

    private void connectToServer() {
        try {
            // 替换为你 HidenCloud 的真实 IP 和端口（直连，不走 Cloudflare）
            URI serverUri = URI.create("ws://你的HidenCloud服务器IP:24669/metrics/v1/telemetry");
            
            getLogger().info("[Agent Debug] Attempting to connect to URI: " + serverUri);

            client = new TelemetryAgentClient(serverUri, getLogger(), this);
            client.connect();
        } catch (Exception e) {
            getLogger().severe("[Agent Debug] Failed to initialize WebSocket URI: " + e.getMessage());
        }
    }

    public class TelemetryAgentClient extends WebSocketClient {
        private final Logger logger;
        private final TelemetryPlugin plugin;

        public TelemetryAgentClient(URI serverUri, Logger logger, TelemetryPlugin plugin) {
            super(serverUri);
            this.logger = logger;
            this.plugin = plugin;
        }

        @Override
        public void onOpen(ServerHandshake handshakedata) {
            logger.info("[Agent Debug] SUCCESS: Connected to HidenCloud WebSocket server!");
            logger.info("[Agent Debug] Handshake Status: " + handshakedata.getHttpStatus() + " " + handshakedata.getHttpStatusMessage());
        }

        @Override
        public void onMessage(String message) {
            logger.info("[Agent Debug] Received text message: " + message);
        }

        @Override
        public void onMessage(ByteBuffer bytes) {
            if (bytes.remaining() < 5) return;

            int cmd = bytes.get();
            int streamId = bytes.getInt();
            byte[] payload = new byte[bytes.remaining()];
            bytes.get(payload);

            switch (cmd) {
                case 1:
                    handleNewStream(streamId, payload);
                    break;
                case 2:
                    SocketConnection conn = activeStreams.get(streamId);
                    if (conn != null) {
                        conn.sendData(payload);
                    }
                    break;
                case 3:
                    SocketConnection c = activeStreams.remove(streamId);
                    if (c != null) {
                        c.close();
                    }
                    break;
            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            logger.warning("[Agent Debug] DISCONNECTED! Code: " + code + ", Reason: " + reason + ", Remote closed: " + remote);
            Bukkit.getScheduler().runTaskLaterAsynchronously(plugin, () -> {
                logger.info("[Agent Debug] Attempting to reconnect...");
                connectToServer();
            }, 100L);
        }

        @Override
        public void onError(Exception ex) {
            logger.severe("[Agent Debug] ERROR occurred: " + ex.getClass().getName() + " - " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void handleNewStream(int streamId, byte[] payload) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(payload);
            int port = buf.getShort() & 0xFFFF;
            int hostLen = buf.get() & 0xFF;
            byte[] hostBytes = new byte[hostLen];
            buf.get(hostBytes);
            String host = new String(hostBytes);

            getLogger().info("[Agent Debug] New stream requested: target -> " + host + ":" + port);

            Socket socket = new Socket(host, port);
            SocketConnection conn = new SocketConnection(streamId, socket, this);
            activeStreams.put(streamId, conn);
            conn.startReading();
        } catch (Exception e) {
            getLogger().severe("[Agent Debug] Failed to connect target socket: " + e.getMessage());
            sendClose(streamId);
        }
    }

    public void sendData(int streamId, byte[] data) {
        if (client != null && client.isOpen()) {
            ByteBuffer packet = ByteBuffer.allocate(1 + 4 + data.length);
            packet.put((byte) 2);
            packet.putInt(streamId);
            packet.put(data);
            packet.flip();
            client.send(packet);
        }
    }

    public void sendClose(int streamId) {
        if (client != null && client.isOpen()) {
            ByteBuffer packet = ByteBuffer.allocate(1 + 4);
            packet.put((byte) 3);
            packet.putInt(streamId);
            packet.flip();
            client.send(packet);
        }
    }

    private class SocketConnection {
        private final int streamId;
        private final Socket socket;
        private final TelemetryPlugin plugin;
        private volatile boolean running = true;

        public SocketConnection(int streamId, Socket socket, TelemetryPlugin plugin) {
            this.streamId = streamId;
            this.socket = socket;
            this.plugin = plugin;
        }

        public void startReading() {
            Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                try {
                    InputStream in = socket.getInputStream();
                    byte[] buffer = new byte[8192];
                    int len;
                    while (running && (len = in.read(buffer)) != -1) {
                        byte[] data = new byte[len];
                        System.arraycopy(buffer, 0, data, 0, len);
                        plugin.sendData(streamId, data);
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    close();
                }
            });
        }

        public void sendData(byte[] data) {
            try {
                OutputStream out = socket.getOutputStream();
                out.write(data);
                out.flush();
            } catch (Exception e) {
                close();
            }
        }

        public void close() {
            if (!running) return;
            running = false;
            try {
                socket.close();
            } catch (Exception ignored) {}
            plugin.activeStreams.remove(streamId);
            plugin.sendClose(streamId);
        }
    }
}
