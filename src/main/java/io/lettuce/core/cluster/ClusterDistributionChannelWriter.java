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
package io.lettuce.core.cluster;

import static io.lettuce.core.cluster.SlotHash.getSlot;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterConnectionProvider.Intent;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.internal.HostAndPort;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.*;

/**
 * Channel writer for cluster operation. This writer looks up the right partition by hash/slot for the operation.
 *
 * @author Mark Paluch
 * @since 3.0
 */
class ClusterDistributionChannelWriter implements RedisChannelWriter {
    //默认写入器
    private final RedisChannelWriter defaultWriter;
    //集群事件监听器
    private final ClusterEventListener clusterEventListener;
    private final int executionLimit;
    //集群连接提供器
    private ClusterConnectionProvider clusterConnectionProvider;
    //异步集群连接提供器
    private AsyncClusterConnectionProvider asyncClusterConnectionProvider;
    //是否关闭
    private boolean closed = false;
    //分区信息
    private volatile Partitions partitions;

    ClusterDistributionChannelWriter(ClientOptions clientOptions, RedisChannelWriter defaultWriter,
            ClusterEventListener clusterEventListener) {

        if (clientOptions instanceof ClusterClientOptions) {
            this.executionLimit = ((ClusterClientOptions) clientOptions).getMaxRedirects();
        } else {
            this.executionLimit = 5;
        }

        this.defaultWriter = defaultWriter;
        this.clusterEventListener = clusterEventListener;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command) {

        LettuceAssert.notNull(command, "Command must not be null");
        //如果连接已经关闭则抛出异常
        if (closed) {
            throw new RedisException("Connection is closed");
        }
        //如果是集群命令且命令没有处理完毕
        if (command instanceof ClusterCommand && !command.isDone()) {
            //类型转换， 转换为ClusterCommand
            ClusterCommand<K, V, T> clusterCommand = (ClusterCommand<K, V, T>) command;
            if (clusterCommand.isMoved() || clusterCommand.isAsk()) {

                HostAndPort target;
                boolean asking;
                //如果集群命令已经迁移,此时通过ClusterCommand中到重试操作进行到此
                if (clusterCommand.isMoved()) {
                    //获取命令迁移目标节点
                    target = getMoveTarget(clusterCommand.getError());
                    //触发迁移事件
                    clusterEventListener.onMovedRedirection();
                    asking = false;
                } else {
                    target = getAskTarget(clusterCommand.getError());
                    asking = true;
                    //触发asking事件
                    clusterEventListener.onAskRedirection();
                }

                command.getOutput().setError((String) null);
                //连接迁移后的目标节点
                CompletableFuture<StatefulRedisConnection<K, V>> connectFuture = asyncClusterConnectionProvider
                        .getConnectionAsync(ClusterConnectionProvider.Intent.WRITE, target.getHostText(), target.getPort());
                //成功建立连接,则向该节点发送命令
                if (isSuccessfullyCompleted(connectFuture)) {
                    writeCommand(command, asking, connectFuture.join(), null);
                } else {
                    connectFuture.whenComplete((connection, throwable) -> writeCommand(command, asking, connection, throwable));
                }

                return command;
            }
        }
        //不是集群命令就是RedisCommand，第一个请求命令就是非ClusterCommand
         //将当前命令包装为集群命令
        ClusterCommand<K, V, T> commandToSend = getCommandToSend(command);
        //获取命令参数
        CommandArgs<K, V> args = command.getArgs();

        //排除集群路由的cluster命令
        if (args != null && !CommandType.CLIENT.equals(commandToSend.getType())) {
            //获取第一个编码后的key
            ByteBuffer encodedKey = args.getFirstEncodedKey();
            //如果encodedKey不为null
            if (encodedKey != null) {
                //获取slot值
                int hash = getSlot(encodedKey);
                //根据命令类型获取命令意图 是读还是写
                ClusterConnectionProvider.Intent intent = getIntent(command.getType());
                //根据意图和slot获取连接
                CompletableFuture<StatefulRedisConnection<K, V>> connectFuture = ((AsyncClusterConnectionProvider) clusterConnectionProvider)
                        .getConnectionAsync(intent, hash);
                //如果成功获取连接
                if (isSuccessfullyCompleted(connectFuture)) {
                    writeCommand(commandToSend, false, connectFuture.join(), null);
                } else {//如果连接尚未处理完，或有异常，则添加完成处理器
                    connectFuture.whenComplete((connection, throwable) -> writeCommand(commandToSend, false, connection,
                            throwable));
                }

                return commandToSend;
            }
        }

        writeCommand(commandToSend, defaultWriter);

        return commandToSend;
    }

    private static boolean isSuccessfullyCompleted(CompletableFuture<?> connectFuture) {
        return connectFuture.isDone() && !connectFuture.isCompletedExceptionally();
    }

    /**
     * 将当前命令包装为集群命令
     */
    private <K, V, T> ClusterCommand<K, V, T> getCommandToSend(RedisCommand<K, V, T> command) {

        //如果是集群命令则直接返回
        if (command instanceof ClusterCommand) {
            return (ClusterCommand<K, V, T>) command;
        }
        //否则对当前命令进行包装
        return new ClusterCommand<>(command, this, executionLimit);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> void writeCommand(RedisCommand<K, V, ?> command, boolean asking,
            StatefulRedisConnection<K, V> connection, Throwable throwable) {

        if (throwable != null) {
            command.completeExceptionally(throwable);
            return;
        }

        try {
            //如果需要asking则发送asking
            if (asking) {
                connection.async().asking();
            }
            //发送命令
            writeCommand(command, ((RedisChannelHandler<K, V>) connection).getChannelWriter());
        } catch (Exception e) {
            command.completeExceptionally(e);
        }
    }

    private static <K, V> void writeCommand(RedisCommand<K, V, ?> command, RedisChannelWriter writer) {

        try {
            getWriterToUse(writer).write(command);
        } catch (Exception e) {
            command.completeExceptionally(e);
        }
    }

    private static RedisChannelWriter getWriterToUse(RedisChannelWriter writer) {

        RedisChannelWriter writerToUse = writer;

        if (writer instanceof ClusterDistributionChannelWriter) {
            //获取默认写入器
            writerToUse = ((ClusterDistributionChannelWriter) writer).defaultWriter;
        }
        return writerToUse;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Collection<RedisCommand<K, V, ?>> write(Collection<? extends RedisCommand<K, V, ?>> commands) {

        LettuceAssert.notNull(commands, "Commands must not be null");

        if (closed) {
            throw new RedisException("Connection is closed");
        }
        //集群命令集合
        List<ClusterCommand<K, V, ?>> clusterCommands = new ArrayList<>(commands.size());
        //默认命令集合
        List<ClusterCommand<K, V, ?>> defaultCommands = new ArrayList<>(commands.size());
        //命令分区映射关系
        Map<SlotIntent, List<ClusterCommand<K, V, ?>>> partitions = new HashMap<>();

        //获取当前集合意图
        Intent intent = getIntent(commands);

        //遍历命令
        for (RedisCommand<K, V, ?> cmd : commands) {
            //如果是集群命令则添加到集群命令集合中
            if (cmd instanceof ClusterCommand) {
                clusterCommands.add((ClusterCommand) cmd);
                continue;
            }
            //如果不是集群命令
            //获取命令参数
            CommandArgs<K, V> args = cmd.getArgs();
            //如果参数不为null则获取第一个key的编码
            ByteBuffer firstEncodedKey = args != null ? args.getFirstEncodedKey() : null;

            //如果第一个编码key为null则添加到默认命令集合中，此时命令一般都是管理命令
            if (firstEncodedKey == null) {
                //构建集群命令并添加到默认命令集合中
                defaultCommands.add(new ClusterCommand<>(cmd, this, executionLimit));
                continue;
            }
            //存在key且不是集群命令

            //获取key的slot
            int hash = getSlot(args.getFirstEncodedKey());

            //如果映射表中不存在则添加到映射表中
            List<ClusterCommand<K, V, ?>> commandPartition = partitions.computeIfAbsent(SlotIntent.of(intent, hash),
                    slotIntent -> new ArrayList<>());
            commandPartition.add(new ClusterCommand<>(cmd, this, executionLimit));
        }
        //遍历所有分区
        for (Map.Entry<SlotIntent, List<ClusterCommand<K, V, ?>>> entry : partitions.entrySet()) {

            //获取每个分区到slotIntent
            SlotIntent slotIntent = entry.getKey();
            //根据意图和slot获取连接
            RedisChannelHandler<K, V> connection = (RedisChannelHandler<K, V>) clusterConnectionProvider.getConnection(
                    slotIntent.intent, slotIntent.slotHash);
            //获取writer
            RedisChannelWriter channelWriter = connection.getChannelWriter();
            if (channelWriter instanceof ClusterDistributionChannelWriter) {
                ClusterDistributionChannelWriter writer = (ClusterDistributionChannelWriter) channelWriter;
                channelWriter = writer.defaultWriter;
            }
            //如果channelWriter不为null且channelWriter不是当前writer也不是默认writer则写入命名集
            if (channelWriter != null && channelWriter != this && channelWriter != defaultWriter) {
                channelWriter.write(entry.getValue());
            }
        }
        //集合命令逐个发送
        clusterCommands.forEach(this::write);
        //使用defaultWriter批量发送
        defaultCommands.forEach(defaultWriter::write);

        return (Collection) commands;
    }

    /**
     * Optimization: Determine command intents and optimize for bulk execution preferring one node.
     * <p>
     * If there is only one intent, then we take the intent derived from the commands. If there is more than one intent, then
     * use {@link Intent#WRITE}.
     *
     * @param commands {@link Collection} of {@link RedisCommand commands}.
     * @return the intent.
     */
    static Intent getIntent(Collection<? extends RedisCommand<?, ?, ?>> commands) {

        boolean w = false;
        boolean r = false;
        Intent singleIntent = Intent.WRITE;

        for (RedisCommand<?, ?, ?> command : commands) {
            //忽略集合命令
            if (command instanceof ClusterCommand) {
                continue;
            }
            //获取当前命令意图赋值给singleIntent
            singleIntent = getIntent(command.getType());
            if (singleIntent == Intent.READ) {
                r = true;
            }

            if (singleIntent == Intent.WRITE) {
                w = true;
            }
            //如果同时存在读写命令就返回意图为写
            if (r && w) {
                return Intent.WRITE;
            }
        }
        //当前意图，如果都是读则返回读意图，如果存在写则返回写意图
        return singleIntent;
    }

    private static Intent getIntent(ProtocolKeyword type) {
        return ReadOnlyCommands.isReadOnlyCommand(type) ? Intent.READ : Intent.WRITE;
    }

    static HostAndPort getMoveTarget(String errorMessage) {

        LettuceAssert.notEmpty(errorMessage, "ErrorMessage must not be empty");
        LettuceAssert.isTrue(errorMessage.startsWith(CommandKeyword.MOVED.name()), "ErrorMessage must start with "
                + CommandKeyword.MOVED);

        String[] movedMessageParts = errorMessage.split(" ");
        LettuceAssert.isTrue(movedMessageParts.length >= 3, "ErrorMessage must consist of 3 tokens (" + errorMessage + ")");

        return HostAndPort.parseCompat(movedMessageParts[2]);
    }

    static HostAndPort getAskTarget(String errorMessage) {

        LettuceAssert.notEmpty(errorMessage, "ErrorMessage must not be empty");
        LettuceAssert.isTrue(errorMessage.startsWith(CommandKeyword.ASK.name()), "ErrorMessage must start with "
                + CommandKeyword.ASK);

        String[] movedMessageParts = errorMessage.split(" ");
        LettuceAssert.isTrue(movedMessageParts.length >= 3, "ErrorMessage must consist of 3 tokens (" + errorMessage + ")");

        return HostAndPort.parseCompat(movedMessageParts[2]);
    }

    @Override
    public void close() {

        if (closed) {
            return;
        }

        closed = true;

        if (defaultWriter != null) {
            defaultWriter.close();
        }

        if (clusterConnectionProvider != null) {
            clusterConnectionProvider.close();
            clusterConnectionProvider = null;
        }
    }

    @Override
    public void setConnectionFacade(ConnectionFacade redisChannelHandler) {
        defaultWriter.setConnectionFacade(redisChannelHandler);
    }

    @Override
    public void setAutoFlushCommands(boolean autoFlush) {
        getClusterConnectionProvider().setAutoFlushCommands(autoFlush);
    }

    @Override
    public void flushCommands() {
        getClusterConnectionProvider().flushCommands();
    }

    public ClusterConnectionProvider getClusterConnectionProvider() {
        return clusterConnectionProvider;
    }

    @Override
    public void reset() {
        defaultWriter.reset();
        clusterConnectionProvider.reset();
    }

    public void setClusterConnectionProvider(ClusterConnectionProvider clusterConnectionProvider) {
        this.clusterConnectionProvider = clusterConnectionProvider;
        this.asyncClusterConnectionProvider = (AsyncClusterConnectionProvider) clusterConnectionProvider;
    }

    public void setPartitions(Partitions partitions) {

        this.partitions = partitions;

        if (clusterConnectionProvider != null) {
            clusterConnectionProvider.setPartitions(partitions);
        }
    }

    public Partitions getPartitions() {
        return partitions;
    }

    /**
     * Set from which nodes data is read. The setting is used as default for read operations on this connection. See the
     * documentation for {@link ReadFrom} for more information.
     *
     * @param readFrom the read from setting, must not be {@literal null}
     */
    public void setReadFrom(ReadFrom readFrom) {
        clusterConnectionProvider.setReadFrom(readFrom);
    }

    /**
     * Gets the {@link ReadFrom} setting for this connection. Defaults to {@link ReadFrom#MASTER} if not set.
     *
     * @return the read from setting
     */
    public ReadFrom getReadFrom() {
        return clusterConnectionProvider.getReadFrom();
    }

    static class SlotIntent {

        final int slotHash;
        final Intent intent;
        private final static SlotIntent[] READ;
        private final static SlotIntent[] WRITE;

        static {
            READ = new SlotIntent[SlotHash.SLOT_COUNT];
            WRITE = new SlotIntent[SlotHash.SLOT_COUNT];

            IntStream.range(0, SlotHash.SLOT_COUNT).forEach(i -> {

                READ[i] = new SlotIntent(i, Intent.READ);
                WRITE[i] = new SlotIntent(i, Intent.WRITE);
            });

        }

        private SlotIntent(int slotHash, Intent intent) {
            this.slotHash = slotHash;
            this.intent = intent;
        }

        public static SlotIntent of(Intent intent, int slot) {

            if (intent == Intent.READ) {
                return READ[slot];
            }

            return WRITE[slot];
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof SlotIntent))
                return false;

            SlotIntent that = (SlotIntent) o;

            if (slotHash != that.slotHash)
                return false;
            return intent == that.intent;
        }

        @Override
        public int hashCode() {
            int result = slotHash;
            result = 31 * result + intent.hashCode();
            return result;
        }
    }
}
