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

import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisChannelInitializer;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceSets;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * 重连处理器
 * @author Mark Paluch
 */
class ReconnectionHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ReconnectionHandler.class);

    //执行异常类型
    private static final Set<Class<?>> EXECUTION_EXCEPTION_TYPES = LettuceSets.unmodifiableSet(TimeoutException.class,
            CancellationException.class, RedisCommandTimeoutException.class, ConnectException.class);
    //客户端选项
    private final ClientOptions clientOptions;
    private final Bootstrap bootstrap;
    //socketAddress提供器
    private final Supplier<SocketAddress> socketAddressSupplier;
    //定时器
    private final Timer timer;
    private final ExecutorService reconnectWorkers;
    private final ConnectionFacade connectionFacade;

    private TimeUnit timeoutUnit = TimeUnit.SECONDS;
    private long timeout = 60;

    private volatile ChannelFuture currentFuture;
    private volatile boolean reconnectSuspended;

    ReconnectionHandler(ClientOptions clientOptions, Bootstrap bootstrap, Supplier<SocketAddress> socketAddressSupplier,
            Timer timer, ExecutorService reconnectWorkers, ConnectionFacade connectionFacade) {

        LettuceAssert.notNull(socketAddressSupplier, "SocketAddressSupplier must not be null");
        LettuceAssert.notNull(bootstrap, "Bootstrap must not be null");
        LettuceAssert.notNull(timer, "Timer must not be null");
        LettuceAssert.notNull(reconnectWorkers, "ExecutorService must not be null");
        LettuceAssert.notNull(connectionFacade, "ConnectionFacade must not be null");

        this.socketAddressSupplier = socketAddressSupplier;
        this.bootstrap = bootstrap;
        this.clientOptions = clientOptions;
        this.timer = timer;
        this.reconnectWorkers = reconnectWorkers;
        this.connectionFacade = connectionFacade;
    }

    /**
     * Initiate reconnect and return a {@link ChannelFuture} for synchronization. The resulting future either succeeds or fails.
     * It can be {@link ChannelFuture#cancel(boolean) canceled} to interrupt reconnection and channel initialization. A failed
     * {@link ChannelFuture} will close the channel.
     *
     * @return reconnect {@link ChannelFuture}.
     */
    protected ChannelFuture reconnect() {
        //获取远程地址
        SocketAddress remoteAddress = socketAddressSupplier.get();

        logger.debug("Reconnecting to Redis at {}", remoteAddress);
        //连接远程地址
        ChannelFuture connectFuture = bootstrap.connect(remoteAddress);
        //初始化channelPromise
        ChannelPromise initFuture = connectFuture.channel().newPromise();
        //添加监听器
        initFuture.addListener((ChannelFuture it) -> {

            if (it.cause() != null) {

                connectFuture.cancel(true);
                close(it.channel());
            }
        });

        connectFuture.addListener((ChannelFuture it) -> {

            if (it.cause() != null) {

                initFuture.tryFailure(it.cause());
                return;
            }

            ChannelPipeline pipeline = it.channel().pipeline();

            RedisChannelInitializer channelInitializer = pipeline.get(RedisChannelInitializer.class);

            if (channelInitializer == null) {

                initFuture.tryFailure(new IllegalStateException(
                        "Reconnection attempt without a RedisChannelInitializer in the channel pipeline"));
                return;
            }

            channelInitializer.channelInitialized().whenComplete(
                    (state, throwable) -> {

                        if (throwable != null) {

                            if (isExecutionException(throwable)) {
                                initFuture.tryFailure(throwable);
                                return;
                            }

                            if (clientOptions.isCancelCommandsOnReconnectFailure()) {
                                connectionFacade.reset();
                            }

                            if (clientOptions.isSuspendReconnectOnProtocolFailure()) {

                                logger.error("Disabling autoReconnect due to initialization failure", throwable);
                                setReconnectSuspended(true);
                            }

                            initFuture.tryFailure(throwable);

                            return;
                        }

                        if (logger.isDebugEnabled()) {
                            logger.info("Reconnected to {}, Channel {}", remoteAddress,
                                    ChannelLogDescriptor.logDescriptor(it.channel()));
                        } else {
                            logger.info("Reconnected to {}", remoteAddress);
                        }

                        initFuture.trySuccess();
                    });
        });

        //超时处理，标记initFuture失败
        Runnable timeoutAction = () -> {
            initFuture.tryFailure(new TimeoutException(String.format("Reconnection attempt exceeded timeout of %d %s ",
                    timeout, timeoutUnit)));
        };

        Timeout timeoutHandle = timer.newTimeout(it -> {

            if (connectFuture.isDone() && initFuture.isDone()) {
                return;
            }

            if (reconnectWorkers.isShutdown()) {
                timeoutAction.run();
                return;
            }
            //向线程池提交超时处理
            reconnectWorkers.submit(timeoutAction);

        }, this.timeout, timeoutUnit);

        initFuture.addListener(it -> timeoutHandle.cancel());

        return this.currentFuture = initFuture;
    }

    private void close(Channel channel) {
        if (channel != null) {
            channel.close();
        }
    }

    boolean isReconnectSuspended() {
        return reconnectSuspended;
    }

    void setReconnectSuspended(boolean reconnectSuspended) {
        this.reconnectSuspended = reconnectSuspended;
    }

    long getTimeout() {
        return timeout;
    }

    void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    void prepareClose() {

        ChannelFuture currentFuture = this.currentFuture;
        if (currentFuture != null && !currentFuture.isDone()) {
            currentFuture.cancel(true);
        }
    }

    /**
     * @param throwable
     * @return {@literal true} if {@code throwable} is an execution {@link Exception}.
     */
    public static boolean isExecutionException(Throwable throwable) {

        for (Class<?> type : EXECUTION_EXCEPTION_TYPES) {
            if (type.isAssignableFrom(throwable.getClass())) {
                return true;
            }
        }

        return false;
    }

    ClientOptions getClientOptions() {
        return clientOptions;
    }
}
