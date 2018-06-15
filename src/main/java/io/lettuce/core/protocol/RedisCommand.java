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

import io.lettuce.core.output.CommandOutput;
import io.netty.buffer.ByteBuf;

/**
 * A redis command that holds an output, arguments and a state, whether it is completed or not.
 *
 * Commands can be wrapped. Outer commands have to notify inner commands but inner commands do not communicate with outer
 * commands.
 *
 * @author Mark Paluch
 * @param <K> Key type.
 * @param <V> Value type.
 * @param <T> Output type.
 * @since 3.0
 */
public interface RedisCommand<K, V, T> {

    /**
     * 命令的结果输出，可以为null
     *
     * @return the command output.
     */
    CommandOutput<K, V, T> getOutput();

    /**
     *命令是否处理结束
     */
    void complete();

    /**
     * 取消命令
     */
    void cancel();

    /**
     * 获取命令参数
     */
    CommandArgs<K, V> getArgs();

    /**
     *
     *异常结束
     */
    boolean completeExceptionally(Throwable throwable);

    /**
     * 获取命令关键字
     */
    ProtocolKeyword getType();

    /**
     * 编码命令
     *
     */
    void encode(ByteBuf buf);

    /**
     * 命令是否被取消
     */
    boolean isCancelled();

    /**
     * 命令是否处理完
     */
    boolean isDone();

    /**
     * 设置新的输出
     * @param output the new command output
     * @throws IllegalStateException if the command is cancelled/completed
     */
    void setOutput(CommandOutput<K, V, T> output);
}
