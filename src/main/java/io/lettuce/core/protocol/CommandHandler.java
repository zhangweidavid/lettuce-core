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

import static io.lettuce.core.ConnectionEvents.Activated;
import static io.lettuce.core.ConnectionEvents.PingBeforeActivate;
import static io.lettuce.core.ConnectionEvents.Reset;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceSets;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.resource.ClientResources;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.local.LocalAddress;
import io.netty.util.Recycler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * A netty {@link ChannelHandler} responsible for writing redis commands and reading responses from the server.
 *
 * @author Will Glozer
 * @author Mark Paluch
 * @author Jongyeol Choi
 * @author Grzegorz Szpak
 */
public class CommandHandler extends ChannelDuplexHandler implements HasQueuedCommands {

    /**
     * When we encounter an unexpected IOException we look for these {@link Throwable#getMessage() messages} (because we have no
     * better way to distinguish) and log them at DEBUG rather than WARN, since they are generally caused by unclean client
     * disconnects rather than an actual problem.
     */
    static final Set<String> SUPPRESS_IO_EXCEPTION_MESSAGES = LettuceSets.unmodifiableSet("Connection reset by peer",
            "Broken pipe", "Connection timed out");

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(CommandHandler.class);
    private static final AtomicLong COMMAND_HANDLER_COUNTER = new AtomicLong();
    //客户端选项
    private final ClientOptions clientOptions;
    private final ClientResources clientResources;
    //终端
    private final Endpoint endpoint;
    //命令队列
    private final ArrayDeque<RedisCommand<?, ?, ?>> stack = new ArrayDeque<>();
    //命令处理器ID
    private final long commandHandlerId = COMMAND_HANDLER_COUNTER.incrementAndGet();
    //redis状态机
    private final RedisStateMachine rsm = new RedisStateMachine();
    private final boolean traceEnabled = logger.isTraceEnabled();
    private final boolean debugEnabled = logger.isDebugEnabled();
    //是否可以延迟测量
    private final boolean latencyMetricsEnabled;
    //是否是有界队列
    private final boolean boundedQueues;
    private final BackpressureSource backpressureSource = new BackpressureSource();
    //频道
    Channel channel;
    private ByteBuf buffer;
    //生命周期状态
    private LifecycleState lifecycleState = LifecycleState.NOT_CONNECTED;
    private String logPrefix;
    private PristineFallbackCommand fallbackCommand;
    private boolean pristine;

    /**
     * 初始化一个处理处理来自队列的命令
     *
     * @param clientOptions   client options for this connection, must not be {@literal null}
     * @param clientResources client resources for this connection, must not be {@literal null}
     * @param endpoint        must not be {@literal null}.
     */
    public CommandHandler(ClientOptions clientOptions, ClientResources clientResources, Endpoint endpoint) {

        LettuceAssert.notNull(clientOptions, "ClientOptions must not be null");
        LettuceAssert.notNull(clientResources, "ClientResources must not be null");
        LettuceAssert.notNull(endpoint, "RedisEndpoint must not be null");

        this.clientOptions = clientOptions;
        this.clientResources = clientResources;
        this.endpoint = endpoint;
        this.latencyMetricsEnabled = clientResources.commandLatencyCollector().isEnabled();
        this.boundedQueues = clientOptions.getRequestQueueSize() != Integer.MAX_VALUE;
    }

    public Queue<RedisCommand<?, ?, ?>> getStack() {
        return stack;
    }

    protected void setState(LifecycleState lifecycleState) {

        if (this.lifecycleState != LifecycleState.CLOSED) {
            this.lifecycleState = lifecycleState;
        }
    }

    @Override
    public Collection<RedisCommand<?, ?, ?>> drainQueue() {
        //排干当前stack中的所有命令
        return drainCommands(stack);
    }
    //获取CommandHandler生命周期状态
    protected LifecycleState getState() {
        return lifecycleState;
    }

    //当前对象生命周期状态是否是关闭状态
    public boolean isClosed() {
        return lifecycleState == LifecycleState.CLOSED;
    }

    /**
     * @see io.netty.channel.ChannelInboundHandlerAdapter#channelRegistered(io.netty.channel.ChannelHandlerContext)
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {

        if (isClosed()) {
            logger.debug("{} Dropping register for a closed channel", logPrefix());
        }
        //获取注入的频道
        channel = ctx.channel();

        if (debugEnabled) {
            logPrefix = null;
            logger.debug("{} channelRegistered()", logPrefix());
        }
        //设置生命周期状态为注册状态
        setState(LifecycleState.REGISTERED);
        //分配缓存
        buffer = ctx.alloc().directBuffer(8192 * 8);
        ctx.fireChannelRegistered();
    }

    /**
     * @see io.netty.channel.ChannelInboundHandlerAdapter#channelUnregistered(io.netty.channel.ChannelHandlerContext)
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        //频道取消注册
        if (debugEnabled) {
            logger.debug("{} channelUnregistered()", logPrefix());
        }

        if (channel != null && ctx.channel() != channel) {
            logger.debug("{} My channel and ctx.channel mismatch. Propagating event to other listeners", logPrefix());
            ctx.fireChannelUnregistered();
            return;
        }

        channel = null;
        //释放buffer
        buffer.release();
        //重置
        reset();
        //设置生命周期状态为close
        setState(LifecycleState.CLOSED);
        //关闭状态机
        rsm.close();

        ctx.fireChannelUnregistered();
    }

    /**
     * @see io.netty.channel.ChannelInboundHandlerAdapter#userEventTriggered(io.netty.channel.ChannelHandlerContext, Object)
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt == EnableAutoRead.INSTANCE) {
            channel.config().setAutoRead(true);
        } else if (evt instanceof Reset) {
            reset();
        } else if (evt instanceof PingBeforeActivate) {

            PingBeforeActivate pba = (PingBeforeActivate) evt;

            stack.addFirst(pba.getCommand());
            ctx.writeAndFlush(pba.getCommand());
            return;
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        InternalLogLevel logLevel = InternalLogLevel.WARN;

        if (!stack.isEmpty()) {
            RedisCommand<?, ?, ?> command = stack.poll();
            if (debugEnabled) {
                logger.debug("{} Storing exception in {}", logPrefix(), command);
            }
            logLevel = InternalLogLevel.DEBUG;

            try {
                command.completeExceptionally(cause);
            } catch (Exception ex) {
                logger.warn("{} Unexpected exception during command completion exceptionally: {}", logPrefix, ex.toString(), ex);
            }
        }

        if (channel == null || !channel.isActive() || !isConnected()) {

            if (debugEnabled) {
                logger.debug("{} Storing exception in connectionError", logPrefix());
            }

            logLevel = InternalLogLevel.DEBUG;
            endpoint.notifyException(cause);
        }

        if (cause instanceof IOException && logLevel.ordinal() > InternalLogLevel.INFO.ordinal()) {
            logLevel = InternalLogLevel.INFO;
            if (SUPPRESS_IO_EXCEPTION_MESSAGES.contains(cause.getMessage())) {
                logLevel = InternalLogLevel.DEBUG;
            }
        }

        logger.log(logLevel, "{} Unexpected exception during request: {}", logPrefix, cause.toString(), cause);
    }

    /**
     * @see io.netty.channel.ChannelInboundHandlerAdapter#channelActive(io.netty.channel.ChannelHandlerContext)
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        logPrefix = null;
        pristine = true;
        fallbackCommand = null;

        if (debugEnabled) {
            logger.debug("{} channelActive()", logPrefix());
        }

        setState(LifecycleState.CONNECTED);
        //通道连接是通知终端
        endpoint.notifyChannelActive(ctx.channel());

        super.channelActive(ctx);
        //如果通道不为null则
        if (channel != null) {
            channel.eventLoop().submit((Runnable) () -> channel.pipeline().fireUserEventTriggered(new Activated()));
        }

        if (debugEnabled) {
            logger.debug("{} channelActive() done", logPrefix());
        }
    }

    /**
     * 排干指定队列
     * @return 队列中所有数据
     */
    private static <T> List<T> drainCommands(Queue<T> source) {

        List<T> target = new ArrayList<>(source.size());

        T cmd;
        while ((cmd = source.poll()) != null) {
            target.add(cmd);
        }

        return target;
    }

    /**
     * @see io.netty.channel.ChannelInboundHandlerAdapter#channelInactive(io.netty.channel.ChannelHandlerContext)
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        if (debugEnabled) {
            logger.debug("{} channelInactive()", logPrefix());
        }

        if (channel != null && ctx.channel() != channel) {
            logger.debug("{} My channel and ctx.channel mismatch. Propagating event to other listeners.", logPrefix());
            super.channelInactive(ctx);
            return;
        }

        setState(LifecycleState.DISCONNECTED);
        setState(LifecycleState.DEACTIVATING);

        endpoint.notifyChannelInactive(ctx.channel());
        //排干当前对象中的队列
        endpoint.notifyDrainQueuedCommands(this);

        setState(LifecycleState.DEACTIVATED);

        PristineFallbackCommand command = this.fallbackCommand;
        if (isProtectedMode(command)) {
            onProtectedMode(command.getOutput().getError());
        }

        rsm.reset();

        if (debugEnabled) {
            logger.debug("{} channelInactive() done", logPrefix());
        }

        super.channelInactive(ctx);
    }

    /**
     * @see io.netty.channel.ChannelDuplexHandler#write(io.netty.channel.ChannelHandlerContext, java.lang.Object,
     * io.netty.channel.ChannelPromise)
     */
    @Override
    @SuppressWarnings("unchecked")
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if (debugEnabled) {
            logger.debug("{} write(ctx, {}, promise)", logPrefix(), msg);
        }

        //如果msg实现了RedisCommand接口就表示发送单个命令
        if (msg instanceof RedisCommand) {
            writeSingleCommand(ctx, (RedisCommand<?, ?, ?>) msg, promise);
            return;
        }
        //如果实现了List接口就表示批量发送命令
        if (msg instanceof List) {

            List<RedisCommand<?, ?, ?>> batch = (List<RedisCommand<?, ?, ?>>) msg;
            //如果集合长度为1 还是执行发送单个命令
            if (batch.size() == 1) {

                writeSingleCommand(ctx, batch.get(0), promise);
                return;
            }
            //批处理
            writeBatch(ctx, batch, promise);
            return;
        }

        if (msg instanceof Collection) {
            writeBatch(ctx, (Collection<RedisCommand<?, ?, ?>>) msg, promise);
        }
    }

    private void writeSingleCommand(ChannelHandlerContext ctx, RedisCommand<?, ?, ?> command, ChannelPromise promise) {

        if (!isWriteable(command)) {
            promise.trySuccess();
            return;
        }
        //入队
        addToStack(command, promise);
        ctx.write(command, promise);
    }

    private void writeBatch(ChannelHandlerContext ctx, Collection<RedisCommand<?, ?, ?>> batch, ChannelPromise promise) {

        Collection<RedisCommand<?, ?, ?>> deduplicated = new LinkedHashSet<>(batch.size(), 1);

        for (RedisCommand<?, ?, ?> command : batch) {

            if (isWriteable(command) && !deduplicated.add(command)) {
                deduplicated.remove(command);
                command.completeExceptionally(new RedisException(
                        "Attempting to write duplicate command that is already enqueued: " + command));
            }
        }

        try {
            validateWrite(deduplicated.size());
        } catch (Exception e) {

            for (RedisCommand<?, ?, ?> redisCommand : deduplicated) {
                redisCommand.completeExceptionally(e);
            }

            throw e;
        }

        for (RedisCommand<?, ?, ?> command : deduplicated) {
            addToStack(command, promise);
        }

        if (!deduplicated.isEmpty()) {
            ctx.write(deduplicated, promise);
        } else {
            promise.trySuccess();
        }
    }

    private void addToStack(RedisCommand<?, ?, ?> command, ChannelPromise promise) {

        try {

            validateWrite(1);

            if (command.getOutput() == null) {
                // fire&forget commands are excluded from metrics
                complete(command);
            }

            RedisCommand<?, ?, ?> redisCommand = potentiallyWrapLatencyCommand(command);

            if (promise.isVoid()) {
                stack.add(redisCommand);
            } else {
                promise.addListener(AddToStack.newInstance(stack, redisCommand));
            }
        } catch (Exception e) {
            command.completeExceptionally(e);
            throw e;
        }
    }

    private void validateWrite(int commands) {

        if (usesBoundedQueues()) {

            // number of maintenance commands (AUTH, CLIENT SETNAME, SELECT, READONLY) should be allowed on top
            // of number of user commands to ensure the driver recovers properly from a disconnect
            int maxMaintenanceCommands = 5;
            int allowedRequestQueueSize = clientOptions.getRequestQueueSize() + maxMaintenanceCommands;
            if (stack.size() + commands > allowedRequestQueueSize)

                throw new RedisException("Internal stack size exceeded: " + clientOptions.getRequestQueueSize()
                        + ". Commands are not accepted until the stack size drops.");
        }
    }

    private boolean usesBoundedQueues() {
        return boundedQueues;
    }

    private static boolean isWriteable(RedisCommand<?, ?, ?> command) {
        return !command.isDone();
    }

    /**
     * 可能包装为延迟测量命令
     *
     */
    private RedisCommand<?, ?, ?> potentiallyWrapLatencyCommand(RedisCommand<?, ?, ?> command) {

        //如果延迟测量不可用则直接返回
        if (!latencyMetricsEnabled) {
            return command;
        }
        //如果当前命令就是延迟命令
        if (command instanceof WithLatency) {

            WithLatency withLatency = (WithLatency) command;
            //重置数据
            withLatency.firstResponse(-1);
            withLatency.sent(nanoTime());

            return command;
        }
        //创建延迟测量命令并设置初始化数据
        LatencyMeteredCommand<?, ?, ?> latencyMeteredCommand = new LatencyMeteredCommand<>(command);
        latencyMeteredCommand.firstResponse(-1);
        latencyMeteredCommand.sent(nanoTime());

        return latencyMeteredCommand;
    }

    /**
     * @see io.netty.channel.ChannelInboundHandlerAdapter#channelRead(io.netty.channel.ChannelHandlerContext, java.lang.Object)
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        ByteBuf input = (ByteBuf) msg;

        if (!input.isReadable() || input.refCnt() == 0) {
            logger.warn("{} Input not readable {}, {}", logPrefix(), input.isReadable(), input.refCnt());
            return;
        }

        if (debugEnabled) {
            logger.debug("{} Received: {} bytes, {} commands in the stack", logPrefix(), input.readableBytes(), stack.size());
        }

        try {
            if (buffer.refCnt() < 1) {
                logger.warn("{} Ignoring received data for closed or abandoned connection", logPrefix());
                return;
            }

            if (debugEnabled && ctx.channel() != channel) {
                logger.debug("{} Ignoring data for a non-registered channel {}", logPrefix(), ctx.channel());
                return;
            }

            if (traceEnabled) {
                logger.trace("{} Buffer: {}", logPrefix(), input.toString(Charset.defaultCharset()).trim());
            }

            buffer.writeBytes(input);

            decode(ctx, buffer);
        } finally {
            input.release();
        }
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer) throws InterruptedException {
          //如果pristine为true，命令队列为空且buffer是可读的，则需要使用回退命令消耗这个响应
        if (pristine && stack.isEmpty() && buffer.isReadable()) {

            if (debugEnabled) {
                logger.debug("{} Received response without a command context (empty stack)", logPrefix());
            }
            //如果耗尽响应失败则将pristine设置为false
            if (consumeResponse(buffer)) {
                pristine = false;
            }

            return;
        }
        //如果命令队列中存在命令或pristine为false或buffer不可读

        //while中只处理命令不为空且buffer可读
        while (canDecode(buffer)) {
             //出对
            RedisCommand<?, ?, ?> command = stack.peek();
            if (debugEnabled) {
                logger.debug("{} Stack contains: {} commands", logPrefix(), stack.size());
            }
            //设置pristine为false
            pristine = false;

            try {
                //如果解码失败则返回
                if (!decode(ctx, buffer, command)) {
                    return;
                }
            } catch (Exception e) {

                ctx.close();
                throw e;
            }

            if (isProtectedMode(command)) {
                onProtectedMode(command.getOutput().getError());
            } else {
                   //是否可以结束命令处理
                if (canComplete(command)) {
                    //如果可以则将命令从队列中删除
                    stack.poll();

                    try {
                        //结束命令
                        complete(command);
                    } catch (Exception e) {
                        logger.warn("{} Unexpected exception during request: {}", logPrefix, e.toString(), e);
                    }
                }
            }
            //解码后置处理
            afterDecode(ctx, command);
        }
           //如果buffer引用数量不为0则抛弃可读字节
        if (buffer.refCnt() != 0) {
            buffer.discardReadBytes();
        }
    }

    /**
     * Decoding hook: Can the buffer be decoded to a command.
     *
     * @param buffer
     * @return
     */
    protected boolean canDecode(ByteBuf buffer) {
        return !stack.isEmpty() && buffer.isReadable();
    }

    /**
     * Decoding hook: Can the command be completed.
     *
     * @param command
     * @return
     */
    protected boolean canComplete(RedisCommand<?, ?, ?> command) {
        return true;
    }

    /**
     * Decoding hook: Complete a command.
     *
     * @param command
     * @see RedisCommand#complete()
     */
    protected void complete(RedisCommand<?, ?, ?> command) {
        command.complete();
    }

    private boolean decode(ChannelHandlerContext ctx, ByteBuf buffer, RedisCommand<?, ?, ?> command) {
        //如果延迟测量可用且命令实现了WithLatency接口
        if (latencyMetricsEnabled && command instanceof WithLatency) {
            //类型强转
            WithLatency withLatency = (WithLatency) command;
            //如果第一个响应时间不为-1则设置当前时间（纳秒）为第一个响应时间
            if (withLatency.getFirstResponse() == -1) {
                withLatency.firstResponse(nanoTime());
            }
            //开始解码，如果解码失败则返回，不记录延迟测量数据
            if (!decode0(ctx, buffer, command)) {
                return false;
            }
            //记录延迟数据
            recordLatency(withLatency, command.getType());

            return true;
        }

        return decode0(ctx, buffer, command);
    }

    private boolean decode0(ChannelHandlerContext ctx, ByteBuf buffer, RedisCommand<?, ?, ?> command) {
            //如果解码失败
        if (!decode(buffer, command, getCommandOutput(command))) {
            //如果命令是Sink命令
            if (command instanceof DemandAware.Sink) {

                DemandAware.Sink sink = (DemandAware.Sink) command;
                sink.setSource(backpressureSource);

                ctx.channel().config().setAutoRead(sink.hasDemand());
            }

            return false;
        }

        if (!ctx.channel().config().isAutoRead()) {
            ctx.channel().config().setAutoRead(true);
        }

        return true;
    }

    /**
     * Decoding hook: Retrieve {@link CommandOutput} for {@link RedisCommand} decoding.
     *
     * @param command
     * @return
     * @see RedisCommand#getOutput()
     */
    protected CommandOutput<?, ?, ?> getCommandOutput(RedisCommand<?, ?, ?> command) {
        return command.getOutput();
    }

    protected boolean decode(ByteBuf buffer, CommandOutput<?, ?, ?> output) {
        return rsm.decode(buffer, output);
    }

    protected boolean decode(ByteBuf buffer, RedisCommand<?, ?, ?> command, CommandOutput<?, ?, ?> output) {
        //使用Redis状态机进行解码
        return rsm.decode(buffer, command, output);
    }

    /**
     * 耗尽一个队列中不存在命令的响应
     *如果buffer解码成功则返回true,否则返回false
     */
    private boolean consumeResponse(ByteBuf buffer) {
        //获取退回命令
        PristineFallbackCommand command = this.fallbackCommand;
        //如果退回命令为null或者未处理结束
        if (command == null || !command.isDone()) {

            if (debugEnabled) {
                logger.debug("{} Consuming response using FallbackCommand", logPrefix());
            }
            //如果退回命令为null则创建一个新的原始的退回命令
            if (command == null) {
                command = new PristineFallbackCommand();
                this.fallbackCommand = command;
            }
            //使用退回命令解码buffer
            if (!decode(buffer, command.getOutput())) {
                return false;
            }
            //
            if (isProtectedMode(command)) {
                onProtectedMode(command.getOutput().getError());
            }
        }

        return true;
    }

    //是否是保护模式
    private boolean isProtectedMode(RedisCommand<?, ?, ?> command) {
        return command != null && command.getOutput() != null && command.getOutput().hasError()
                && RedisConnectionException.isProtectedMode(command.getOutput().getError());
    }

    private void onProtectedMode(String message) {

        RedisConnectionException exception = new RedisConnectionException(message);

        endpoint.notifyException(exception);

        if (channel != null) {
            channel.disconnect();
        }

        stack.forEach(cmd -> cmd.completeExceptionally(exception));
        stack.clear();
    }

    /**
     * Hook method called after command completion.
     *
     * @param ctx
     * @param command
     */
    protected void afterDecode(ChannelHandlerContext ctx, RedisCommand<?, ?, ?> command) {
    }

    private void recordLatency(WithLatency withLatency, ProtocolKeyword commandType) {
        //如果withLatency不为null且命令延迟收集器可用同时channel和remote()不为null
        if (withLatency != null && clientResources.commandLatencyCollector().isEnabled() && channel != null && remote() != null) {
            //第一个响应延迟等于第一个响应时间减去发送时间
            long firstResponseLatency = withLatency.getFirstResponse() - withLatency.getSent();
            //结束时间为当前时间减去发送时间
            long completionLatency = nanoTime() - withLatency.getSent();
            //使用延迟收集器记录数据
            clientResources.commandLatencyCollector().recordCommandLatency(local(), remote(), commandType,
                    firstResponseLatency, completionLatency);
        }
    }

    private SocketAddress remote() {
        return channel.remoteAddress();
    }

    private SocketAddress local() {
        if (channel.localAddress() != null) {
            return channel.localAddress();
        }
        return LocalAddress.ANY;
    }

    boolean isConnected() {
        return lifecycleState.ordinal() >= LifecycleState.CONNECTED.ordinal()
                && lifecycleState.ordinal() < LifecycleState.DISCONNECTED.ordinal();
    }

    private void reset() {

        resetInternals();
        //取消队列中的所有命令
        cancelCommands("Reset", drainCommands(stack));
    }
    //内部重置
    private void resetInternals() {
        //重置状态机
        rsm.reset();
        //清空buffer
        if (buffer.refCnt() > 0) {
            buffer.clear();
        }
    }

    /**
     * 按照指定的异常消息取消指定命令集合
     */
    private static void cancelCommands(String message, List<RedisCommand<?, ?, ?>> toCancel) {

        for (RedisCommand<?, ?, ?> cmd : toCancel) {
            if (cmd.getOutput() != null) {
                //设置错误消息
                cmd.getOutput().setError(message);
            }
            //执行取消
            cmd.cancel();
        }
    }

    private String logPrefix() {

        if (logPrefix != null) {
            return logPrefix;
        }

        String buffer = "[" + ChannelLogDescriptor.logDescriptor(channel) + ", " + "chid=0x"
                + Long.toHexString(commandHandlerId) + ']';
        return logPrefix = buffer;
    }

    private static long nanoTime() {
        return System.nanoTime();
    }

    /**
     * 生命周期状态： 未连接，已注册，已连接等
     */
    public enum LifecycleState {
        NOT_CONNECTED, REGISTERED, CONNECTED, ACTIVATING, ACTIVE, DISCONNECTED, DEACTIVATING, DEACTIVATED, CLOSED,
    }

    /**
     * Source for backpressure.
     */
    class BackpressureSource implements DemandAware.Source {

        @Override
        public void requestMore() {

            if (isConnected() && !isClosed()) {
                if (!channel.config().isAutoRead()) {
                    channel.pipeline().fireUserEventTriggered(EnableAutoRead.INSTANCE);
                }
            }
        }
    }

    enum EnableAutoRead {
        INSTANCE
    }

    /**
     * Add to stack listener. This listener is pooled and must be {@link #recycle() recycled after usage}.
     */
    static class AddToStack implements GenericFutureListener<Future<Void>> {

        private static final Recycler<AddToStack> RECYCLER = new Recycler<AddToStack>() {
            @Override
            protected AddToStack newObject(Handle<AddToStack> handle) {
                return new AddToStack(handle);
            }
        };

        private final Recycler.Handle<AddToStack> handle;
        private ArrayDeque stack;
        private RedisCommand<?, ?, ?> command;

        AddToStack(Recycler.Handle<AddToStack> handle) {
            this.handle = handle;
        }

        /**
         * Allocate a new instance.
         *
         * @param stack
         * @param command
         * @return
         */
        static AddToStack newInstance(ArrayDeque stack, RedisCommand<?, ?, ?> command) {

            AddToStack entry = RECYCLER.get();

            entry.stack = stack;
            entry.command = command;

            return entry;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void operationComplete(Future<Void> future) {

            try {
                if (future.isSuccess()) {
                    stack.add(command);
                }
            } finally {
                recycle();
            }
        }

        private void recycle() {

            this.stack = null;
            this.command = null;

            handle.recycle(this);
        }
    }
}
