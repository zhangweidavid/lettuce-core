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

import static io.lettuce.core.protocol.CommandHandler.SUPPRESS_IO_EXCEPTION_MESSAGES;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import io.lettuce.core.*;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceFactories;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.EncoderException;
import io.netty.util.Recycler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Default {@link Endpoint} implementation.
 *
 * @author Mark Paluch
 */
public class DefaultEndpoint implements RedisChannelWriter, Endpoint {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultEndpoint.class);
    private static final AtomicLong ENDPOINT_COUNTER = new AtomicLong();
    private static final AtomicIntegerFieldUpdater<DefaultEndpoint> QUEUE_SIZE = AtomicIntegerFieldUpdater.newUpdater(
            DefaultEndpoint.class, "queueSize");

    protected volatile Channel channel;

    private final Reliability reliability;
    private final ClientOptions clientOptions;
    private final Queue<RedisCommand<?, ?, ?>> disconnectedBuffer;
    private final Queue<RedisCommand<?, ?, ?>> commandBuffer;
    private final boolean boundedQueues;

    private final long endpointId = ENDPOINT_COUNTER.incrementAndGet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final SharedLock sharedLock = new SharedLock();
    private final boolean debugEnabled = logger.isDebugEnabled();

    private String logPrefix;
    private boolean autoFlushCommands = true;

    private ConnectionWatchdog connectionWatchdog;
    private ConnectionFacade connectionFacade;

    private volatile Throwable connectionError;

    // access via QUEUE_SIZE
    @SuppressWarnings("unused")
    private volatile int queueSize = 0;

    /**
     * Create a new {@link DefaultEndpoint}.
     * 根据clientOptions创建终端
     * @param clientOptions client options for this connection, must not be {@literal null}
     */
    public DefaultEndpoint(ClientOptions clientOptions) {

        LettuceAssert.notNull(clientOptions, "ClientOptions must not be null");

        this.clientOptions = clientOptions;
        //如果设置自动连接，则可靠性选在AT_LEAST_ONCE,否则选在AT_MOST_ONCE
        this.reliability = clientOptions.isAutoReconnect() ? Reliability.AT_LEAST_ONCE : Reliability.AT_MOST_ONCE;
       //根据设置的请求队列大小创建断开连接缓存
        this.disconnectedBuffer = LettuceFactories.newConcurrentQueue(clientOptions.getRequestQueueSize());
        //创建不自动提交的命令缓存
        this.commandBuffer = LettuceFactories.newConcurrentQueue(clientOptions.getRequestQueueSize());
        //是否是有界队列
        this.boundedQueues = clientOptions.getRequestQueueSize() != Integer.MAX_VALUE;
    }

    @Override
    public void setConnectionFacade(ConnectionFacade connectionFacade) {
        this.connectionFacade = connectionFacade;
    }

    //设置是否自动提交
    @Override
    public void setAutoFlushCommands(boolean autoFlush) {
        this.autoFlushCommands = autoFlush;
    }

    //写命令
    @Override
    public <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command) {

        LettuceAssert.notNull(command, "Command must not be null");

        try {
            //写入器计数自增
            sharedLock.incrementWriters();

            //校验写入器
            validateWrite(1);

            //如果自动刷新
            if (autoFlushCommands) {

                if (isConnected()) {
                    //向已连接通道写入命令并刷新
                    writeToChannelAndFlush(command);
                } else {
                    //向未连接通道写入命令并刷新
                    writeToDisconnectedBuffer(command);
                }

            } else {//不是自动刷新则将命令写入到缓存
                writeToBuffer(command);
            }
        } finally {
            sharedLock.decrementWriters();
            if (debugEnabled) {
                logger.debug("{} write() done", logPrefix());
            }
        }

        return command;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Collection<RedisCommand<K, V, ?>> write(Collection<? extends RedisCommand<K, V, ?>> commands) {

        LettuceAssert.notNull(commands, "Commands must not be null");

        try {
            sharedLock.incrementWriters();

            validateWrite(commands.size());

            if (autoFlushCommands) {

                if (isConnected()) {
                    writeToChannelAndFlush(commands);
                } else {
                    writeToDisconnectedBuffer(commands);
                }

            } else {
                writeToBuffer(commands);
            }
        } finally {
            sharedLock.decrementWriters();
            if (debugEnabled) {
                logger.debug("{} write() done", logPrefix());
            }
        }

        return (Collection<RedisCommand<K, V, ?>>) commands;
    }

    private void validateWrite(int commands) {
        //如果已经关闭则抛出异常
        if (isClosed()) {
            throw new RedisException("Connection is closed");
        }
        //是否使用有界队列
        if (usesBoundedQueues()) {
            //当前通道是否已经连接
            boolean connected = isConnected();
            //如果队列满则抛出异常
            if (QUEUE_SIZE.get(this) + commands > clientOptions.getRequestQueueSize()) {
                throw new RedisException("Request queue size exceeded: " + clientOptions.getRequestQueueSize()
                        + ". Commands are not accepted until the queue size drops.");
            }
            //如果通道没有连接同时断开连接缓存已满则抛出异常
            if (!connected && disconnectedBuffer.size() + commands > clientOptions.getRequestQueueSize()) {
                throw new RedisException("Request queue size exceeded: " + clientOptions.getRequestQueueSize()
                        + ". Commands are not accepted until the queue size drops.");
            }
            //已经连接但是命令缓存已满则抛出异常
            if (connected && commandBuffer.size() + commands > clientOptions.getRequestQueueSize()) {
                throw new RedisException("Command buffer size exceeded: " + clientOptions.getRequestQueueSize()
                        + ". Commands are not accepted until the queue size drops.");
            }
        }

        if (!isConnected() && isRejectCommand()) {
            throw new RedisException("Currently not connected. Commands are rejected.");
        }
    }

    private boolean usesBoundedQueues() {
        return boundedQueues;
    }

    private void writeToBuffer(Iterable<? extends RedisCommand<?, ?, ?>> commands) {

        for (RedisCommand<?, ?, ?> command : commands) {
            writeToBuffer(command);
        }
    }

    private void writeToDisconnectedBuffer(Collection<? extends RedisCommand<?, ?, ?>> commands) {
        for (RedisCommand<?, ?, ?> command : commands) {
            writeToDisconnectedBuffer(command);
        }
    }

    private void writeToDisconnectedBuffer(RedisCommand<?, ?, ?> command) {

        if (connectionError != null) {
            if (debugEnabled) {
                logger.debug("{} writeToDisconnectedBuffer() Completing command {} due to connection error", logPrefix(),
                        command);
            }
            command.completeExceptionally(connectionError);

            return;
        }

        if (debugEnabled) {
            logger.debug("{} writeToDisconnectedBuffer() buffering (disconnected) command {}", logPrefix(), command);
        }

        disconnectedBuffer.add(command);
    }

    protected <C extends RedisCommand<?, ?, T>, T> void writeToBuffer(C command) {

        if (debugEnabled) {
            logger.debug("{} writeToBuffer() buffering command {}", logPrefix(), command);
        }

        if (connectionError != null) {

            if (debugEnabled) {
                logger.debug("{} writeToBuffer() Completing command {} due to connection error", logPrefix(), command);
            }
            command.completeExceptionally(connectionError);

            return;
        }

        commandBuffer.add(command);
    }

    private void writeToChannelAndFlush(RedisCommand<?, ?, ?> command) {
         //将单前对象添加到队列中
        QUEUE_SIZE.incrementAndGet(this);
        //如果可靠策略是AT_MOST_ONCE
        if (reliability == Reliability.AT_MOST_ONCE) {
            // cancel on exceptions and remove from queue, because there is no housekeeping
            channelWriteAndFlush(command).addListener(AtMostOnceWriteListener.newInstance(this, command));
        }

        if (reliability == Reliability.AT_LEAST_ONCE) {
            // commands are ok to stay within the queue, reconnect will retrigger them
            channelWriteAndFlush(command).addListener(RetryListener.newInstance(this, command));
        }
    }

    private void writeToChannelAndFlush(Collection<? extends RedisCommand<?, ?, ?>> commands) {

        QUEUE_SIZE.addAndGet(this, commands.size());

        if (reliability == Reliability.AT_MOST_ONCE) {

            // cancel on exceptions and remove from queue, because there is no housekeeping
            for (RedisCommand<?, ?, ?> command : commands) {
                channelWrite(command).addListener(AtMostOnceWriteListener.newInstance(this, command));
            }
        }

        if (reliability == Reliability.AT_LEAST_ONCE) {

            // commands are ok to stay within the queue, reconnect will retrigger them
            for (RedisCommand<?, ?, ?> command : commands) {
                channelWrite(command).addListener(RetryListener.newInstance(this, command));
            }
        }

        channelFlush();
    }

    private void channelFlush() {

        if (debugEnabled) {
            logger.debug("{} write() channelFlush", logPrefix());
        }

        channel.flush();
    }

    private ChannelFuture channelWrite(RedisCommand<?, ?, ?> command) {

        if (debugEnabled) {
            logger.debug("{} write() channelWrite command {}", logPrefix(), command);
        }

        return channel.write(command);
    }

    private ChannelFuture channelWriteAndFlush(RedisCommand<?, ?, ?> command) {

        if (debugEnabled) {
            logger.debug("{} write() writeAndFlush command {}", logPrefix(), command);
        }

        return channel.writeAndFlush(command);
    }

    private boolean isRejectCommand() {

        if (clientOptions == null) {
            return false;
        }

        switch (clientOptions.getDisconnectedBehavior()) {
            case REJECT_COMMANDS:
                return true;

            case ACCEPT_COMMANDS:
                return false;

            default:
            case DEFAULT:
                if (!clientOptions.isAutoReconnect()) {
                    return true;
                }

                return false;
        }
    }

    @Override
    public void notifyChannelActive(Channel channel) {

        this.logPrefix = null;
        this.channel = channel;
        this.connectionError = null;

        if (isClosed()) {

            logger.info("{} Closing channel because endpoint is already closed", logPrefix());
            channel.close();
            return;
        }

        if (connectionWatchdog != null) {
            connectionWatchdog.arm();
        }
        //独占锁执行
        sharedLock.doExclusive(() -> {

            try {
                // Move queued commands to buffer before issuing any commands because of connection activation.
                // That's necessary to prepend queued commands first as some commands might get into the queue
                // after the connection was disconnected. They need to be prepended to the command buffer

                if (debugEnabled) {
                    logger.debug("{} activateEndpointAndExecuteBufferedCommands {} command(s) buffered", logPrefix(),
                            disconnectedBuffer.size());
                }

                if (debugEnabled) {
                    logger.debug("{} activating endpoint", logPrefix());
                }

                connectionFacade.activated();
                //将断开期间的命令都发送出去
                flushCommands(disconnectedBuffer);
            } catch (Exception e) {

                if (debugEnabled) {
                    logger.debug("{} channelActive() ran into an exception", logPrefix());
                }

                if (clientOptions.isCancelCommandsOnReconnectFailure()) {
                    reset();
                }

                throw e;
            }
        });
    }

    @Override
    public void notifyChannelInactive(Channel channel) {

        if (isClosed()) {
            cancelBufferedCommands("Connection closed");
        }

        sharedLock.doExclusive(() -> {

            if (debugEnabled) {
                logger.debug("{} deactivating endpoint handler", logPrefix());
            }

            connectionFacade.deactivated();
        });

        if (this.channel == channel) {
            this.channel = null;
        }
    }

    @Override
    public void notifyException(Throwable t) {

        if (t instanceof RedisConnectionException && RedisConnectionException.isProtectedMode(t.getMessage())) {

            connectionError = t;

            if (connectionWatchdog != null) {
                connectionWatchdog.setListenOnChannelInactive(false);
                connectionWatchdog.setReconnectSuspended(false);
            }

            doExclusive(this::drainCommands).forEach(cmd -> cmd.completeExceptionally(t));
        }

        if (!isConnected()) {
            connectionError = t;
        }
    }

    @Override
    public void registerConnectionWatchdog(ConnectionWatchdog connectionWatchdog) {
        this.connectionWatchdog = connectionWatchdog;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void flushCommands() {
        flushCommands(commandBuffer);
    }

    private void flushCommands(Queue<RedisCommand<?, ?, ?>> queue) {

        if (debugEnabled) {
            logger.debug("{} flushCommands()", logPrefix());
        }

        if (isConnected()) {

            List<RedisCommand<?, ?, ?>> commands = sharedLock.doExclusive(() -> {

                if (queue.isEmpty()) {
                    return Collections.<RedisCommand<?, ?, ?>> emptyList();
                }

                return drainCommands(queue);
            });

            if (debugEnabled) {
                logger.debug("{} flushCommands() Flushing {} commands", logPrefix(), commands.size());
            }

            if (!commands.isEmpty()) {
                writeToChannelAndFlush(commands);
            }
        }
    }

    /**
     * Close the connection.
     */
    @Override
    public void close() {

        if (debugEnabled) {
            logger.debug("{} close()", logPrefix());
        }

        if (isClosed()) {
            return;
        }

        if (closed.compareAndSet(false, true)) {

            if (connectionWatchdog != null) {
                connectionWatchdog.prepareClose();
            }

            cancelBufferedCommands("Close");

            Channel currentChannel = this.channel;
            if (currentChannel != null) {

                ChannelFuture close = currentChannel.close();
                if (currentChannel.isOpen()) {
                    close.syncUninterruptibly();
                }
            }
        }
    }

    /**
     * Reset the writer state. Queued commands will be canceled and the internal state will be reset. This is useful when the
     * internal state machine gets out of sync with the connection.
     */
    @Override
    public void reset() {

        if (debugEnabled) {
            logger.debug("{} reset()", logPrefix());
        }

        if (channel != null) {
            channel.pipeline().fireUserEventTriggered(new ConnectionEvents.Reset());
        }
        cancelBufferedCommands("Reset");
    }

    /**
     * Reset the command-handler to the initial not-connected state.
     */
    public void initialState() {

        commandBuffer.clear();

        Channel currentChannel = this.channel;
        if (currentChannel != null) {

            ChannelFuture close = currentChannel.close();
            if (currentChannel.isOpen()) {
                close.syncUninterruptibly();
            }
        }
    }

    @Override
    public void notifyDrainQueuedCommands(HasQueuedCommands queuedCommands) {

        if (isClosed()) {
            cancelCommands("Connection closed", queuedCommands.drainQueue());
            cancelCommands("Connection closed", drainCommands());
            return;
        }

        sharedLock.doExclusive(() -> {

            Collection<RedisCommand<?, ?, ?>> commands = queuedCommands.drainQueue();

            if (debugEnabled) {
                logger.debug("{} notifyQueuedCommands adding {} command(s) to buffer", logPrefix(), commands.size());
            }

            commands.addAll(drainCommands(disconnectedBuffer));

            for (RedisCommand<?, ?, ?> command : commands) {

                if (command instanceof DemandAware.Sink) {
                    ((DemandAware.Sink) command).removeSource();
                }
            }

            try {
                disconnectedBuffer.addAll(commands);
            } catch (RuntimeException e) {

                if (debugEnabled) {
                    logger.debug("{} notifyQueuedCommands Queue overcommit. Cannot add all commands to buffer (disconnected).",
                            logPrefix(), commands.size());
                }
                commands.removeAll(disconnectedBuffer);

                for (RedisCommand<?, ?, ?> command : commands) {
                    command.completeExceptionally(e);
                }
            }

            if (isConnected()) {
                flushCommands(disconnectedBuffer);
            }
        });
    }

    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Execute a {@link Supplier} callback guarded by an exclusive lock.
     *
     * @param supplier
     * @param <T>
     * @return
     */
    protected <T> T doExclusive(Supplier<T> supplier) {
        return sharedLock.doExclusive(supplier);
    }

    protected List<RedisCommand<?, ?, ?>> drainCommands() {

        List<RedisCommand<?, ?, ?>> target = new ArrayList<>();

        target.addAll(drainCommands(disconnectedBuffer));
        target.addAll(drainCommands(commandBuffer));

        return target;
    }

    private static List<RedisCommand<?, ?, ?>> drainCommands(Queue<? extends RedisCommand<?, ?, ?>> source) {

        List<RedisCommand<?, ?, ?>> target = new ArrayList<>(source.size());

        RedisCommand<?, ?, ?> cmd;
        while ((cmd = source.poll()) != null) {
            target.add(cmd);
        }

        return target;
    }

    private void cancelBufferedCommands(String message) {
        cancelCommands(message, doExclusive(this::drainCommands));
    }

    private void cancelCommands(String message, Iterable<? extends RedisCommand<?, ?, ?>> toCancel) {

        for (RedisCommand<?, ?, ?> cmd : toCancel) {
            if (cmd.getOutput() != null) {
                cmd.getOutput().setError(message);
            }
            cmd.cancel();
        }
    }

    private boolean isConnected() {

        Channel channel = this.channel;
        return channel != null && channel.isActive();
    }

    protected String logPrefix() {

        if (logPrefix != null) {
            return logPrefix;
        }

        String buffer = "[" + ChannelLogDescriptor.logDescriptor(channel) + ", " + "epid=0x" + Long.toHexString(endpointId)
                + ']';
        return logPrefix = buffer;
    }

    //监听器辅助类
    static class ListenerSupport {

        Collection<? extends RedisCommand<?, ?, ?>> sentCommands;
        RedisCommand<?, ?, ?> sentCommand;
        DefaultEndpoint endpoint;

        //出列
        void dequeue() {
            //如果发送到命令不为null则对当前终端出列
            if (sentCommand != null) {
                QUEUE_SIZE.decrementAndGet(endpoint);
            } else {
                QUEUE_SIZE.addAndGet(endpoint, -sentCommands.size());
            }
        }

        protected void complete(Throwable t) {

            if (sentCommand != null) {
                sentCommand.completeExceptionally(t);
            } else {
                for (RedisCommand<?, ?, ?> sentCommand : sentCommands) {
                    sentCommand.completeExceptionally(t);
                }
            }
        }
    }

    static class AtMostOnceWriteListener extends ListenerSupport implements ChannelFutureListener {

        private static final Recycler<AtMostOnceWriteListener> RECYCLER = new Recycler<AtMostOnceWriteListener>() {
            @Override
            protected AtMostOnceWriteListener newObject(Handle<AtMostOnceWriteListener> handle) {
                return new AtMostOnceWriteListener(handle);
            }
        };

        private final Recycler.Handle<AtMostOnceWriteListener> handle;

        AtMostOnceWriteListener(Recycler.Handle<AtMostOnceWriteListener> handle) {
            this.handle = handle;
        }

        static AtMostOnceWriteListener newInstance(DefaultEndpoint endpoint, RedisCommand<?, ?, ?> command) {

            AtMostOnceWriteListener entry = RECYCLER.get();

            entry.endpoint = endpoint;
            entry.sentCommand = command;

            return entry;
        }

        static AtMostOnceWriteListener newInstance(DefaultEndpoint endpoint,
                Collection<? extends RedisCommand<?, ?, ?>> commands) {

            AtMostOnceWriteListener entry = RECYCLER.get();

            entry.endpoint = endpoint;
            entry.sentCommands = commands;

            return entry;
        }

        @Override
        public void operationComplete(ChannelFuture future) {

            try {

                dequeue();

                if (!future.isSuccess() && future.cause() != null) {
                    complete(future.cause());
                }
            } finally {
                recycle();
            }
        }

        private void recycle() {

            this.endpoint = null;
            this.sentCommand = null;
            this.sentCommands = null;

            handle.recycle(this);
        }
    }

    /**
     * 重试监听器
     */
    static class RetryListener extends ListenerSupport implements GenericFutureListener<Future<Void>> {

        private static final Recycler<RetryListener> RECYCLER = new Recycler<RetryListener>() {
            @Override
            protected RetryListener newObject(Handle<RetryListener> handle) {
                return new RetryListener(handle);
            }
        };

        private final Recycler.Handle<RetryListener> handle;

        RetryListener(Recycler.Handle<RetryListener> handle) {
            this.handle = handle;
        }

        static RetryListener newInstance(DefaultEndpoint endpoint, RedisCommand<?, ?, ?> command) {
            //获取重试简体器对象
            RetryListener entry = RECYCLER.get();

            entry.endpoint = endpoint;
            entry.sentCommand = command;

            return entry;
        }

        static RetryListener newInstance(DefaultEndpoint endpoint, Collection<? extends RedisCommand<?, ?, ?>> commands) {

            RetryListener entry = RECYCLER.get();

            entry.endpoint = endpoint;
            entry.sentCommands = commands;

            return entry;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void operationComplete(Future<Void> future) {

            try {
                doComplete(future);
            } finally {
                recycle();
            }
        }

        private void doComplete(Future<Void> future) {

            Throwable cause = future.cause();

            boolean success = future.isSuccess();
            //出列
            dequeue();
            //如果写成功则返回
            if (success) {
                return;
            }
            //如果写异常是指定对异常则不再重试
            if (cause instanceof EncoderException || cause instanceof Error || cause.getCause() instanceof Error) {
                complete(cause);
                return;
            }

            //其它异常则重试
            //获取通道
            Channel channel = endpoint.channel;

            //获取发送的命令
            RedisCommand<?, ?, ?> sentCommand = this.sentCommand;
            //获取发送的批量命令
            Collection<? extends RedisCommand<?, ?, ?>> sentCommands = this.sentCommands;
            //尝试入队
            potentiallyRequeueCommands(channel, sentCommand, sentCommands);

            if (!(cause instanceof ClosedChannelException)) {

                String message = "Unexpected exception during request: {}";
                InternalLogLevel logLevel = InternalLogLevel.WARN;

                if (cause instanceof IOException && SUPPRESS_IO_EXCEPTION_MESSAGES.contains(cause.getMessage())) {
                    logLevel = InternalLogLevel.DEBUG;
                }

                logger.log(logLevel, message, cause.toString(), cause);
            }
        }

        /**
         * Requeue command/commands
         *
         * @param channel
         * @param sentCommand
         * @param sentCommands
         */
        private void potentiallyRequeueCommands(Channel channel, RedisCommand<?, ?, ?> sentCommand,
                Collection<? extends RedisCommand<?, ?, ?>> sentCommands) {
            //如果发送命令不为null且已经处理完成，则返回
            if (sentCommand != null && sentCommand.isDone()) {
                return;
            }

            if (sentCommands != null) {

                boolean foundToSend = false;

                for (RedisCommand<?, ?, ?> command : sentCommands) {
                    if (!command.isDone()) {
                        foundToSend = true;
                        break;
                    }
                }

                if (!foundToSend) {
                    return;
                }
            }
            //存在通道
            if (channel != null) {
                //获取终端
                DefaultEndpoint endpoint = this.endpoint;
                //向channel工作线程池提交任务
                channel.eventLoop().submit(() -> {
                    requeueCommands(sentCommand, sentCommands, endpoint);
                });
            } else {
                requeueCommands(sentCommand, sentCommands, endpoint);
            }
        }

        @SuppressWarnings("unchecked")
        private void requeueCommands(RedisCommand<?, ?, ?> sentCommand, Collection sentCommands, DefaultEndpoint endpoint) {

            if (sentCommand != null) {
                try {
                    endpoint.write(sentCommand);
                } catch (Exception e) {
                    sentCommand.completeExceptionally(e);
                }
            } else {
                try {
                    endpoint.write(sentCommands);
                } catch (Exception e) {
                    for (RedisCommand<?, ?, ?> command : (Collection<RedisCommand>) sentCommands) {
                        command.completeExceptionally(e);
                    }
                }
            }
        }

        private void recycle() {

            this.endpoint = null;
            this.sentCommand = null;
            this.sentCommands = null;

            handle.recycle(this);
        }
    }

    private enum Reliability {
        AT_MOST_ONCE, AT_LEAST_ONCE
    }

}
