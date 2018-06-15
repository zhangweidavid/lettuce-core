/*
 * Copyright 2011-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.protocol;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.ConnectionEvents;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.resource.Delay;
import io.lettuce.core.resource.Delay.StatefulDelay;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * A netty {@link ChannelHandler} responsible for monitoring the channel and reconnecting when the connection is lost.
 *
 * @author Will Glozer
 * @author Mark Paluch
 * @author Koji Lin
 */
@ChannelHandler.Sharable
public class ConnectionWatchdog extends ChannelInboundHandlerAdapter {

    //日志不起作用时间间隔
    private static final long LOGGING_QUIET_TIME_MS = TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS);
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ConnectionWatchdog.class);

    private final Delay reconnectDelay;
    private final Bootstrap bootstrap;
    private final EventExecutorGroup reconnectWorkers;
    private final ReconnectionHandler reconnectionHandler;
    private final ReconnectionListener reconnectionListener;
    private final Timer timer;

    private Channel channel;
    //远程地址
    private SocketAddress remoteAddress;
    //最后一次重连时间
    private long lastReconnectionLogging = -1;
    //日志前缀
    private String logPrefix;
    //是否同步重连
    private final AtomicBoolean reconnectSchedulerSync;
    private volatile int attempts;
    //是否准备好
    private volatile boolean armed;
    private volatile boolean listenOnChannelInactive;
    //重连超时时间
    private volatile Timeout reconnectScheduleTimeout;

    /**
     * Create a new watchdog that adds to new connections to the supplied {@link ChannelGroup} and establishes a new
     * {@link Channel} when disconnected, while reconnect is true. The socketAddressSupplier can supply the reconnect address.
     *
     * @param reconnectDelay reconnect delay, must not be {@literal null}
     * @param clientOptions client options for the current connection, must not be {@literal null}
     * @param bootstrap Configuration for new channels, must not be {@literal null}
     * @param timer Timer used for delayed reconnect, must not be {@literal null}
     * @param reconnectWorkers executor group for reconnect tasks, must not be {@literal null}
     * @param socketAddressSupplier the socket address supplier to obtain an address for reconnection, may be {@literal null}
     * @param reconnectionListener the reconnection listener, must not be {@literal null}
     * @param connectionFacade the connection facade, must not be {@literal null}
     */
    public ConnectionWatchdog(Delay reconnectDelay, ClientOptions clientOptions, Bootstrap bootstrap, Timer timer,
            EventExecutorGroup reconnectWorkers, Supplier<SocketAddress> socketAddressSupplier,
            ReconnectionListener reconnectionListener, ConnectionFacade connectionFacade) {

        LettuceAssert.notNull(reconnectDelay, "Delay must not be null");
        LettuceAssert.notNull(clientOptions, "ClientOptions must not be null");
        LettuceAssert.notNull(bootstrap, "Bootstrap must not be null");
        LettuceAssert.notNull(timer, "Timer must not be null");
        LettuceAssert.notNull(reconnectWorkers, "ReconnectWorkers must not be null");
        LettuceAssert.notNull(reconnectionListener, "ReconnectionListener must not be null");
        LettuceAssert.notNull(connectionFacade, "ConnectionFacade must not be null");

        this.reconnectDelay = reconnectDelay;
        this.bootstrap = bootstrap;
        this.timer = timer;
        this.reconnectWorkers = reconnectWorkers;
        this.reconnectionListener = reconnectionListener;
        this.reconnectSchedulerSync = new AtomicBoolean(false);

        Supplier<SocketAddress> wrappedSocketAddressSupplier = new Supplier<SocketAddress>() {
            @Override
            public SocketAddress get() {

                if (socketAddressSupplier != null) {
                    try {
                        remoteAddress = socketAddressSupplier.get();
                    } catch (RuntimeException e) {
                        logger.warn("Cannot retrieve the current address from socketAddressSupplier: " + e.toString()
                                + ", reusing old address " + remoteAddress);
                    }
                }

                return remoteAddress;
            }
        };
        //创建重连处理器
        this.reconnectionHandler = new ReconnectionHandler(clientOptions, bootstrap, wrappedSocketAddressSupplier, timer,
                reconnectWorkers, connectionFacade);

        resetReconnectDelay();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        logger.debug("{} userEventTriggered(ctx, {})", logPrefix(), evt);

        if (evt instanceof ConnectionEvents.Activated) {
            attempts = 0;
            resetReconnectDelay();
        }

        super.userEventTriggered(ctx, evt);
    }

    /**
     * 主备关闭
     */
    void prepareClose() {
        //设置不监听断开连接
        setListenOnChannelInactive(false);
        //暂停重新连接
        setReconnectSuspended(true);
        //获取重连的超时时间
        Timeout reconnectScheduleTimeout = this.reconnectScheduleTimeout;
        //如果超时时间不为null且没有被取消则执行取消
        if (reconnectScheduleTimeout != null && !reconnectScheduleTimeout.isCancelled()) {
            reconnectScheduleTimeout.cancel();
        }

        reconnectionHandler.prepareClose();
    }

    //通道激活
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //同步重连设置为false
        reconnectSchedulerSync.set(false);
        //设置通道
        channel = ctx.channel();
        //重连超时时间设置为null
        reconnectScheduleTimeout = null;
        //日志前缀设置为null
        logPrefix = null;
        //获取远程地址
        remoteAddress = channel.remoteAddress();
        //重复
        logPrefix = null;
        logger.debug("{} channelActive()", logPrefix());

        super.channelActive(ctx);
    }

    //通道不可用
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        logger.debug("{} channelInactive()", logPrefix());
        if (!armed) {
            logger.debug("{} ConnectionWatchdog not armed", logPrefix());
            return;
        }
        //将当前channel设置为null
        channel = null;
            //如果监听channelInactive同时重连处理器不是延迟连接的
        if (listenOnChannelInactive && !reconnectionHandler.isReconnectSuspended()) {
            scheduleReconnect();
        } else {
            logger.debug("{} Reconnect scheduling disabled", logPrefix(), ctx);
        }

        super.channelInactive(ctx);
    }

    /**
     * 让ConnectionWatchDog开始监听频道失效事件
     */
    void arm() {
        //已经武装好
        this.armed = true;
        //开始监听频道失效事件
        setListenOnChannelInactive(true);
    }

    /**
     * 如果通道不可用则延后重连
     */
    public void scheduleReconnect() {

        logger.debug("{} scheduleReconnect()", logPrefix());

        if (!isEventLoopGroupActive()) {
            logger.debug("isEventLoopGroupActive() == false");
            return;
        }

        if (!isListenOnChannelInactive()) {
            logger.debug("Skip reconnect scheduling, listener disabled");
            return;
        }

        if ((channel == null || !channel.isActive()) && reconnectSchedulerSync.compareAndSet(false, true)) {
            //尝试次数
            attempts++;
            final int attempt = attempts;
            //获取延迟时间
            int timeout = (int) reconnectDelay.createDelay(attempt).toMillis();
            logger.debug("{} Reconnect attempt {}, delay {}ms", logPrefix(), attempt, timeout);

            //timer执行重连，并将任务关联具柄赋值给reconnectScheduleTimeout
            this.reconnectScheduleTimeout = timer.newTimeout(it -> {

                reconnectScheduleTimeout = null;

                if (!isEventLoopGroupActive()) {
                    logger.warn("Cannot execute scheduled reconnect timer, reconnect workers are terminated");
                    return;
                }

                reconnectWorkers.submit(() -> {
                    ConnectionWatchdog.this.run(attempt);
                    return null;
                });
            }, timeout, TimeUnit.MILLISECONDS);

            //获取同步定时重连标志为false，则将具柄设置为null
            if (!reconnectSchedulerSync.get()) {
                reconnectScheduleTimeout = null;
            }
        } else {
            logger.debug("{} Skipping scheduleReconnect() because I have an active channel", logPrefix());
        }
    }

    /**
     * Reconnect to the remote address that the closed channel was connected to. This creates a new {@link ChannelPipeline} with
     * the same handler instances contained in the old channel's pipeline.
     *
     * @param attempt attempt counter
     *
     * @throws Exception when reconnection fails.
     */
    public void run(int attempt) throws Exception {

        reconnectSchedulerSync.set(false);
        reconnectScheduleTimeout = null;

        if (!isEventLoopGroupActive()) {
            logger.debug("isEventLoopGroupActive() == false");
            return;
        }

        if (!isListenOnChannelInactive()) {
            logger.debug("Skip reconnect scheduling, listener disabled");
            return;
        }

        if (isReconnectSuspended()) {
            logger.debug("Skip reconnect scheduling, reconnect is suspended");
            return;
        }

        boolean shouldLog = shouldLog();

        InternalLogLevel infoLevel = InternalLogLevel.INFO;
        InternalLogLevel warnLevel = InternalLogLevel.WARN;

        if (shouldLog) {//需要记录，则将当前时间赋值个lastReconnectionLogging
            lastReconnectionLogging = System.currentTimeMillis();
        } else {//否则设置日志级别为DEBUG
            warnLevel = InternalLogLevel.DEBUG;
            infoLevel = InternalLogLevel.DEBUG;
        }

        InternalLogLevel warnLevelToUse = warnLevel;

        try {
            //通知重连监听器重新连接
            reconnectionListener.onReconnect(new ConnectionEvents.Reconnect(attempt));
            //日志输出
            logger.log(infoLevel, "Reconnecting, last destination was {}", remoteAddress);

            //重连处理器执行重连
            ChannelFuture future = reconnectionHandler.reconnect();
            //向重连channelFuture注册监听器
            future.addListener(it -> {
                //成功或无异常则返回
                if (it.isSuccess() || it.cause() == null) {
                    return;
                }
                //异常
                Throwable throwable = it.cause();
                //如果是执行异常则打印日志
                if (ReconnectionHandler.isExecutionException(throwable)) {
                    logger.log(warnLevelToUse, "Cannot reconnect: {}", throwable.toString());
                } else {//非执行异常，打印日志输出异常栈
                    logger.log(warnLevelToUse, "Cannot reconnect: {}", throwable.toString(), throwable);
                }
                //没有暂停重连，则延后重连
                if (!isReconnectSuspended()) {
                    scheduleReconnect();
                }
            });
        } catch (Exception e) {
            logger.log(warnLevel, "Cannot reconnect: {}", e.toString());
        }
    }

    private boolean isEventLoopGroupActive() {

        if (!isEventLoopGroupActive(bootstrap.group()) || !isEventLoopGroupActive(reconnectWorkers)) {
            return false;
        }

        return true;
    }

    private static boolean isEventLoopGroupActive(EventExecutorGroup executorService) {
        return !(executorService.isShuttingDown());
    }

    private boolean shouldLog() {
        //下一个记录时间
        long quietUntil = lastReconnectionLogging + LOGGING_QUIET_TIME_MS;
        //如果当前时间大于或等于下一次记录时间点则表示需要记录
        return quietUntil <= System.currentTimeMillis();
    }

    /**
     * Enable event listener for disconnected events.
     *
     * @param listenOnChannelInactive {@literal true} to listen for disconnected events.
     */
    public void setListenOnChannelInactive(boolean listenOnChannelInactive) {
        this.listenOnChannelInactive = listenOnChannelInactive;
    }

    public boolean isListenOnChannelInactive() {
        return listenOnChannelInactive;
    }

    /**
     * Suspend reconnection temporarily. Reconnect suspension will interrupt reconnection attempts.
     *
     * @param reconnectSuspended {@literal true} to suspend reconnection
     */
    public void setReconnectSuspended(boolean reconnectSuspended) {
        reconnectionHandler.setReconnectSuspended(reconnectSuspended);
    }

    public boolean isReconnectSuspended() {
        return reconnectionHandler.isReconnectSuspended();
    }

    ReconnectionHandler getReconnectionHandler() {
        return reconnectionHandler;
    }

    private void resetReconnectDelay() {
        if (reconnectDelay instanceof StatefulDelay) {
            ((StatefulDelay) reconnectDelay).reset();
        }
    }

    private String logPrefix() {

        if (logPrefix != null) {
            return logPrefix;
        }

        String
        buffer= "[" +ChannelLogDescriptor.logDescriptor(channel)+", last known addr="+remoteAddress+']';
        return logPrefix = buffer;
    }
}
