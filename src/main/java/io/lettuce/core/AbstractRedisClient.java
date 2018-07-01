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
package io.lettuce.core;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.io.Closeable;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.lettuce.core.Transports.NativeTransports;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.ConnectionWatchdog;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.ConcurrentSet;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Base Redis client. This class holds the netty infrastructure, {@link ClientOptions} and the basic connection procedure. This
 * class creates the netty {@link EventLoopGroup}s for NIO ({@link NioEventLoopGroup}) and EPoll (
 * {@link io.netty.channel.epoll.EpollEventLoopGroup}) with a default of {@code Runtime.getRuntime().availableProcessors() * 4}
 * threads. Reuse the instance as much as possible since the {@link EventLoopGroup} instances are expensive and can consume a
 * huge part of your resources, if you create multiple instances.
 * <p>
 * You can set the number of threads per {@link NioEventLoopGroup} by setting the {@code io.netty.eventLoopThreads} system
 * property to a reasonable number of threads.
 * </p>
 *
 * @author Mark Paluch
 * @author Jongyeol Choi
 * @since 3.0
 * @see ClientResources
 */
public abstract class AbstractRedisClient {
    //池化的ByteBuf分配器
    protected static final PooledByteBufAllocator BUF_ALLOCATOR = PooledByteBufAllocator.DEFAULT;

    protected static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisClient.class);
    //对象线程池映射表
    protected final Map<Class<? extends EventLoopGroup>, EventLoopGroup> eventLoopGroups = new ConcurrentHashMap<>(2);
    //连接事件
    protected final ConnectionEvents connectionEvents = new ConnectionEvents();
    //可关闭资源
    protected final Set<Closeable> closeableResources = new ConcurrentSet<>();
    //通用线程池
    protected final EventExecutorGroup genericWorkerPool;
    //事件轮定时器
    protected final HashedWheelTimer timer;
    //频道组，所有有效到频道都会添加到频道组中
    protected final ChannelGroup channels;
    //客户端资源
    protected final ClientResources clientResources;
    //客户端选项，
    protected volatile ClientOptions clientOptions = ClientOptions.builder().build();
    //超时
    protected Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;
    //是否共享资源，如果使用默认资源就不是共享资源，如果使用用户配置的资源就是共享资源
    private final boolean sharedResources;
    //是否关闭
    private final AtomicBoolean shutdown = new AtomicBoolean();

    /**
     * 使用指定的资源创建客户端
     */
    protected AbstractRedisClient(ClientResources clientResources) {
        //如果指定的资源为null则使用默认资源，同时设置是否共享的资源为false
        if (clientResources == null) {
            sharedResources = false;
            this.clientResources = DefaultClientResources.create();
        } else {//指定的资源不为null则是共享的资源
            sharedResources = true;
            this.clientResources = clientResources;
        }
        //通用的工作线程池是clientResource的eventExecutorGroup
        genericWorkerPool = this.clientResources.eventExecutorGroup();
        //使用受genericWrokerPool管理的线程池新创建频道组
        channels = new DefaultChannelGroup(genericWorkerPool.next());
        timer = (HashedWheelTimer) this.clientResources.timer();
    }

    /**
     * Set the default timeout for connections created by this client. The timeout applies to connection attempts and
     * non-blocking commands.
     *
     * @param timeout default connection timeout, must not be {@literal null}.
     * @since 5.0
     */
    public void setDefaultTimeout(Duration timeout) {

        LettuceAssert.notNull(timeout, "Timeout duration must not be null");
        LettuceAssert.isTrue(!timeout.isNegative(), "Timeout duration must be greater or equal to zero");

        this.timeout = timeout;
    }

    /**
     * Set the default timeout for connections created by this client. The timeout applies to connection attempts and
     * non-blocking commands.
     *
     * @param timeout Default connection timeout.
     * @param unit Unit of time for the timeout.
     * @deprecated since 5.0, use {@link #setDefaultTimeout(Duration)}.
     */
    @Deprecated
    public void setDefaultTimeout(long timeout, TimeUnit unit) {
        setDefaultTimeout(Duration.ofNanos(unit.toNanos(timeout)));
    }

    /**
     * Populate connection builder with necessary resources.
     *
     * @param socketAddressSupplier address supplier for initial connect and re-connect
     * @param connectionBuilder connection builder to configure the connection
     * @param redisURI URI of the Redis instance
     */
    protected void connectionBuilder(Supplier<SocketAddress> socketAddressSupplier, ConnectionBuilder connectionBuilder,
            RedisURI redisURI) {
       //创建Bootstrap netty启动器
        Bootstrap redisBootstrap = new Bootstrap();
        //设置channel选项
        redisBootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
        redisBootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);

        redisBootstrap.option(ChannelOption.ALLOCATOR, BUF_ALLOCATOR);
        //获取套接字选项
        SocketOptions socketOptions = getOptions().getSocketOptions();
        //设置连接超时时间
        redisBootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                Math.toIntExact(socketOptions.getConnectTimeout().toMillis()));
        //如果redisURI中没有socket选择参数则根据clientresouce设置
        if (LettuceStrings.isEmpty(redisURI.getSocket())) {
            //是否保持长连接
            redisBootstrap.option(ChannelOption.SO_KEEPALIVE, socketOptions.isKeepAlive());
            //是否要求TCP低延迟
            redisBootstrap.option(ChannelOption.TCP_NODELAY, socketOptions.isTcpNoDelay());
        }
        //设置超时时间
        connectionBuilder.timeout(redisURI.getTimeout());
        //设置密码
        connectionBuilder.password(redisURI.getPassword());
        //设置bootstrap
        connectionBuilder.bootstrap(redisBootstrap);
        connectionBuilder.channelGroup(channels).connectionEvents(connectionEvents).timer(timer);
        connectionBuilder.socketAddressSupplier(socketAddressSupplier);
    }

    protected void channelType(ConnectionBuilder connectionBuilder, ConnectionPoint connectionPoint) {

        LettuceAssert.notNull(connectionPoint, "ConnectionPoint must not be null");
        //设置客户端线程组，EventLoopGroup用来处理所有频道事件
        connectionBuilder.bootstrap().group(getEventLoopGroup(connectionPoint));

        if (connectionPoint.getSocket() != null) {
            NativeTransports.assertAvailable();
            connectionBuilder.bootstrap().channel(NativeTransports.domainSocketChannelClass());
        } else {
            connectionBuilder.bootstrap().channel(Transports.socketChannelClass());
        }
    }

    private synchronized EventLoopGroup getEventLoopGroup(ConnectionPoint connectionPoint) {

        //如果connectionPoint中的套接字不为null同时eventLoopGroups中不存在传输类型则创建一个新的eventLoopGroup
        if (connectionPoint.getSocket() == null && !eventLoopGroups.containsKey(Transports.eventLoopGroupClass())) {
            eventLoopGroups.put(Transports.eventLoopGroupClass(),
                    clientResources.eventLoopGroupProvider().allocate(Transports.eventLoopGroupClass()));
        }
        //如果套接字不为null
        if (connectionPoint.getSocket() != null) {

            NativeTransports.assertAvailable();

            Class<? extends EventLoopGroup> eventLoopGroupClass = NativeTransports.eventLoopGroupClass();

            if (!eventLoopGroups.containsKey(NativeTransports.eventLoopGroupClass())) {
                eventLoopGroups
                        .put(eventLoopGroupClass, clientResources.eventLoopGroupProvider().allocate(eventLoopGroupClass));
            }
        }

        if (connectionPoint.getSocket() == null) {
            return eventLoopGroups.get(Transports.eventLoopGroupClass());
        }

        if (connectionPoint.getSocket() != null) {
            NativeTransports.assertAvailable();
            return eventLoopGroups.get(NativeTransports.eventLoopGroupClass());
        }

        throw new IllegalStateException("This should not have happened in a binary decision. Please file a bug.");
    }

    /**
     * Retrieve the connection from {@link ConnectionFuture}. Performs a blocking {@link ConnectionFuture#get()} to synchronize
     * the channel/connection initialization. Any exception is rethrown as {@link RedisConnectionException}.
     *
     * @param connectionFuture must not be null.
     * @param <T> Connection type.
     * @return the connection.
     * @throws RedisConnectionException in case of connection failures.
     * @since 4.4
     */
    protected <T> T getConnection(ConnectionFuture<T> connectionFuture) {

        try {
            //获取connection
            return connectionFuture.get();
        } catch (InterruptedException e) {//中断异常
            //中断当前线程
            Thread.currentThread().interrupt();
            //抛出连接异常
            throw RedisConnectionException.create(connectionFuture.getRemoteAddress(), e);
        } catch (Exception e) {
            if (e instanceof ExecutionException) {
                throw RedisConnectionException.create(connectionFuture.getRemoteAddress(), e.getCause());
            }

            throw RedisConnectionException.create(connectionFuture.getRemoteAddress(), e);
        }
    }

    /**
     *  异步处理连接同时通过connectionBuilder初始化一个通道
     */
    @SuppressWarnings("unchecked")
    protected <K, V, T extends RedisChannelHandler<K, V>> ConnectionFuture<T> initializeChannelAsync(
            ConnectionBuilder connectionBuilder) {
        //获取socketAddress
        SocketAddress redisAddress = connectionBuilder.socketAddress();

        //如果线程池关闭则抛出异常
        if (clientResources.eventExecutorGroup().isShuttingDown()) {
            throw new IllegalStateException("Cannot connect, Event executor group is terminated.");
        }

        logger.debug("Connecting to Redis at {}", redisAddress);
        //频道准备就绪future
        CompletableFuture<Channel> channelReadyFuture = new CompletableFuture<>();

        //获取bootstrap
        Bootstrap redisBootstrap = connectionBuilder.bootstrap();
        //创建redis通道初始化器
        RedisChannelInitializer initializer = connectionBuilder.build();
        //设置netty的处理器
        redisBootstrap.handler(initializer);
        //netty自定设置处理
        clientResources.nettyCustomizer().afterBootstrapInitialized(redisBootstrap);
        CompletableFuture<Boolean> initFuture = initializer.channelInitialized();
        //连接Redis服务器，在该处才是真正和服务器创建连接
        ChannelFuture connectFuture = redisBootstrap.connect(redisAddress);
        //增加监听器
        connectFuture.addListener(future -> {
            //没有成功
            if (!future.isSuccess()) {

                logger.debug("Connecting to Redis at {}: {}", redisAddress, future.cause());
                connectionBuilder.endpoint().initialState();
                //通过准备就绪异步结果异常结束
                channelReadyFuture.completeExceptionally(future.cause());
                return;
            }
            //completableFuture特性，在future结束的时候执行
            initFuture.whenComplete((success, throwable) -> {
                //如果throwable不为null表示存在异常
                if (throwable == null) {
                    logger.debug("Connecting to Redis at {}: Success", redisAddress);
                    //获取RedisChannelHandler
                    RedisChannelHandler<?, ?> connection = connectionBuilder.connection();
                    //注册可关闭资源，在connection关闭的时候关闭可关闭资源
                    connection.registerCloseables(closeableResources, connection);
                    //频道准备就绪
                    channelReadyFuture.complete(connectFuture.channel());
                    return;
                }

                logger.debug("Connecting to Redis at {}, initialization: {}", redisAddress, throwable);
                connectionBuilder.endpoint().initialState();
                Throwable failure;

                if (throwable instanceof RedisConnectionException) {
                    failure = throwable;
                } else if (throwable instanceof TimeoutException) {
                    failure = new RedisConnectionException("Could not initialize channel within "
                            + connectionBuilder.getTimeout(), throwable);
                } else {
                    failure = throwable;
                }
                channelReadyFuture.completeExceptionally(failure);

                CompletableFuture<Boolean> response = new CompletableFuture<>();
                response.completeExceptionally(failure);

            });
        });
        //针对connectionBuilder.connection()的结果进行装饰，增加获取remoteAddress功能
        return new DefaultConnectionFuture<T>(redisAddress, channelReadyFuture.thenApply(channel -> (T) connectionBuilder
                .connection()));
    }

    /**
     * Shutdown this client and close all open connections. The client should be discarded after calling shutdown. The shutdown
     * has no quiet time and a timeout of 2 seconds.
     */
    public void shutdown() {
        shutdown(0, 2, TimeUnit.SECONDS);
    }

    /**
     * Shutdown this client and close all open connections. The client should be discarded after calling shutdown.
     *
     * @param quietPeriod the quiet period as described in the documentation
     * @param timeout the maximum amount of time to wait until the executor is shutdown regardless if a task was submitted
     *        during the quiet period
     * @since 5.0
     */
    public void shutdown(Duration quietPeriod, Duration timeout) {
        shutdown(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Shutdown this client and close all open connections. The client should be discarded after calling shutdown.
     *
     * @param quietPeriod the quiet period as described in the documentation
     * @param timeout the maximum amount of time to wait until the executor is shutdown regardless if a task was submitted
     *        during the quiet period
     * @param timeUnit the unit of {@code quietPeriod} and {@code timeout}
     */
    public void shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {

        try {
            shutdownAsync(quietPeriod, timeout, timeUnit).get();
        } catch (RuntimeException e) {
            throw e;
        } catch (ExecutionException e) {

            if (e.getCause() instanceof RedisCommandExecutionException) {
                throw new RedisCommandExecutionException(e.getCause().getMessage(), e.getCause());
            }

            throw new RedisException(e.getCause());
        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
            throw new RedisCommandInterruptedException(e);
        } catch (Exception e) {
            throw new RedisCommandExecutionException(e);
        }
    }

    /**
     * Shutdown this client and close all open connections asynchronously. The client should be discarded after calling
     * shutdown. The shutdown has 2 secs quiet time and a timeout of 15 secs.
     *
     * @since 4.4
     */
    public CompletableFuture<Void> shutdownAsync() {
        return shutdownAsync(2, 15, TimeUnit.SECONDS);
    }

    /**
     * Shutdown this client and close all open connections asynchronously. The client should be discarded after calling
     * shutdown.
     *
     * @param quietPeriod the quiet period as described in the documentation
     * @param timeout the maximum amount of time to wait until the executor is shutdown regardless if a task was submitted
     *        during the quiet period
     * @param timeUnit the unit of {@code quietPeriod} and {@code timeout}
     * @since 4.4
     */
    @SuppressWarnings("rawtypes")
    public CompletableFuture<Void> shutdownAsync(long quietPeriod, long timeout, TimeUnit timeUnit) {

        if (shutdown.compareAndSet(false, true)) {

            while (!closeableResources.isEmpty()) {
                Closeable closeableResource = closeableResources.iterator().next();
                try {
                    closeableResource.close();
                } catch (Exception e) {
                    logger.debug("Exception on Close: " + e.getMessage(), e);
                }
                closeableResources.remove(closeableResource);
            }

            List<CompletableFuture<Void>> closeFutures = new ArrayList<>();

            //遍历所有频道
            for (Channel c : channels) {

                ChannelPipeline pipeline = c.pipeline();

                ConnectionWatchdog commandHandler = pipeline.get(ConnectionWatchdog.class);
                if (commandHandler != null) {
                    commandHandler.setListenOnChannelInactive(false);
                }
            }

            try {
                closeFutures.add(toCompletableFuture(channels.close()));
            } catch (Exception e) {
                logger.debug("Cannot close channels", e);
            }

            if (!sharedResources) {
                Future<?> groupCloseFuture = clientResources.shutdown(quietPeriod, timeout, timeUnit);
                closeFutures.add(toCompletableFuture(groupCloseFuture));
            } else {
                for (EventLoopGroup eventExecutors : eventLoopGroups.values()) {
                    Future<?> groupCloseFuture = clientResources.eventLoopGroupProvider().release(eventExecutors, quietPeriod,
                            timeout, timeUnit);
                    closeFutures.add(toCompletableFuture(groupCloseFuture));
                }
            }

            return allOf(closeFutures.toArray(new CompletableFuture[0]));
        }

        return completedFuture(null);
    }

    protected int getResourceCount() {
        return closeableResources.size();
    }

    protected int getChannelCount() {
        return channels.size();
    }

    /**
     * Add a listener for the RedisConnectionState. The listener is notified every time a connect/disconnect/IO exception
     * happens. The listeners are not bound to a specific connection, so every time a connection event happens on any
     * connection, the listener will be notified. The corresponding netty channel handler (async connection) is passed on the
     * event.
     *
     * @param listener must not be {@literal null}
     */
    public void addListener(RedisConnectionStateListener listener) {
        LettuceAssert.notNull(listener, "RedisConnectionStateListener must not be null");
        connectionEvents.addListener(listener);
    }

    /**
     * Removes a listener.
     *
     * @param listener must not be {@literal null}
     */
    public void removeListener(RedisConnectionStateListener listener) {

        LettuceAssert.notNull(listener, "RedisConnectionStateListener must not be null");
        connectionEvents.removeListener(listener);
    }

    /**
     * Returns the {@link ClientOptions} which are valid for that client. Connections inherit the current options at the moment
     * the connection is created. Changes to options will not affect existing connections.
     *
     * @return the {@link ClientOptions} for this client
     */
    public ClientOptions getOptions() {
        return clientOptions;
    }

    /**
     * Set the {@link ClientOptions} for the client.
     *
     * @param clientOptions client options for the client and connections that are created after setting the options
     */
    protected void setOptions(ClientOptions clientOptions) {
        LettuceAssert.notNull(clientOptions, "ClientOptions must not be null");
        this.clientOptions = clientOptions;
    }

    private static CompletableFuture<Void> toCompletableFuture(Future<?> future) {

        CompletableFuture<Void> promise = new CompletableFuture<>();

        if (future.isDone() || future.isCancelled()) {
            if (future.isSuccess()) {
                promise.complete(null);
            } else {
                promise.completeExceptionally(future.cause());
            }
            return promise;
        }

        future.addListener(f -> {
            if (f.isSuccess()) {
                promise.complete(null);
            } else {
                promise.completeExceptionally(f.cause());
            }
        });

        return promise;
    }
}
