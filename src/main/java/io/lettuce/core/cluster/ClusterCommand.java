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

import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.protocol.*;
import io.netty.buffer.ByteBuf;

/**
 * @author Mark Paluch
 * @since 3.0
 */
class ClusterCommand<K, V, T> extends CommandWrapper<K, V, T> implements RedisCommand<K, V, T> {
    //重定向次数
    private int redirections;
    //最大重定向次数
    private final int maxRedirections;
    //重试写入器
    private final RedisChannelWriter retry;
    //是否处理结束
    private boolean completed;

    /**
     *
     * @param command
     * @param retry
     * @param maxRedirections
     */
    ClusterCommand(RedisCommand<K, V, T> command, RedisChannelWriter retry, int maxRedirections) {
        super(command);
        this.retry = retry;
        this.maxRedirections = maxRedirections;
    }

    @Override
    public void complete() {
        //如果响应是MOVED或ASK
        if (isMoved() || isAsk()) {
            //如果最大重定向次数大于当前重定向次数则可以进行重定向
            boolean retryCommand = maxRedirections > redirections;
            //重定向次数自增
            redirections++;

            if (retryCommand) {
                try {
                    //重定向
                    retry.write(this);
                } catch (Exception e) {
                    completeExceptionally(e);
                }
                return;
            }
        }
        super.complete();
        completed = true;
    }

    public boolean isMoved() {
        //如果错误消息不为null且错误消息是以 MOVED开头
        if (getError() != null && getError().startsWith(CommandKeyword.MOVED.name())) {
            return true;
        }

        return false;
    }

    public boolean isAsk() {
        //如果错误消息不为null且错误消息是以ASK开头
        if (getError() != null && getError().startsWith(CommandKeyword.ASK.name())) {
            return true;
        }

        return false;
    }

    @Override
    public CommandArgs<K, V> getArgs() {
        return command.getArgs();
    }

    @Override
    public void encode(ByteBuf buf) {
        command.encode(buf);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        boolean result = command.completeExceptionally(ex);
        completed = true;
        return result;
    }

    @Override
    public ProtocolKeyword getType() {
        return command.getType();
    }

    public boolean isCompleted() {
        return completed;
    }

    @Override
    public boolean isDone() {
        return isCompleted();
    }

    //获取错误消息
    public String getError() {
        if (command.getOutput() != null) {
            return command.getOutput().getError();
        }
        return null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [command=").append(command);
        sb.append(", redirections=").append(redirections);
        sb.append(", maxRedirections=").append(maxRedirections);
        sb.append(']');
        return sb.toString();
    }
}
