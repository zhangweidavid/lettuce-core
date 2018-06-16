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

import java.io.Closeable;
import java.util.Collection;

import io.lettuce.core.protocol.ConnectionFacade;
import io.lettuce.core.protocol.RedisCommand;

/**
 * 频道写入器，写入器向通信频道推送命令并为之命令的状态
 * @author Mark Paluch
 * @since 3.0
 */
public interface RedisChannelWriter extends Closeable {

    /**
     * 向频道中写入命令，命令在写入期间可能被改变或包装，在调用之后返回写入实例
     *
     * @param command redis命令
     * @param <T> 返回类型
     * @return 写入的redis命令
     */
    <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command);

    /**
     * Write multiple commands on the channel. The commands may be changed/wrapped during write and the written instance is
     * returned after the call.
     *
     * @param commands the Redis commands.
     * @param <K> key type
     * @param <V> value type
     * @return the written redis command
     */
    <K, V> Collection<RedisCommand<K, V, ?>> write(Collection<? extends RedisCommand<K, V, ?>> commands);

    @Override
    void close();

    /**
     * 重置写入器状态，排队的命令将被取消同时内部状态将被重置。对于内部状态机异步获异步连接的输出这是很有用的
     */
    void reset();

    /**
     * Set the corresponding connection facade in order to notify it about channel active/inactive state.
     *
     * @param connection the connection facade (external connection object)
     */
    void setConnectionFacade(ConnectionFacade connection);

    /**
     * Disable or enable auto-flush behavior. Default is {@literal true}. If autoFlushCommands is disabled, multiple commands
     * can be issued without writing them actually to the transport. Commands are buffered until a {@link #flushCommands()} is
     * issued. After calling {@link #flushCommands()} commands are sent to the transport and executed by Redis.
     *
     * @param autoFlush state of autoFlush.
     */
    void setAutoFlushCommands(boolean autoFlush);

    /**
     * Flush pending commands. This commands forces a flush on the channel and can be used to buffer ("pipeline") commands to
     * achieve batching. No-op if channel is not connected.
     */
    void flushCommands();
}
