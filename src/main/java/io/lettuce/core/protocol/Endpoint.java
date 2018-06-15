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

import io.netty.channel.Channel;

/**
 *  包装一个抽象基础频道都有状态断点；断点可能是连接，断开连接，关闭等状态；断点可能具有重发排队命令的重连接功能
 * @author Mark Paluch
 */
public interface Endpoint {

    /**
     * 通知当前频道已经激活
     *
     * @param channel the channel
     */
    void notifyChannelActive(Channel channel);

    /**
     * 通知单前频道失效
     *
     * @param channel the channel
     */
    void notifyChannelInactive(Channel channel);

    /**
     * 通知在频道或命令处理过程中发生异常
     * @param t the Exception
     */
    void notifyException(Throwable t);

    /**
     * 通知终端抛弃所有排队命令
     *
     * @param queuedCommands the queue holder.
     */
    void notifyDrainQueuedCommands(HasQueuedCommands queuedCommands);

    /**
     * 将ConnnectionWatchDog和单前终端关联起来
     * @param connectionWatchdog the connection watchdog.
     */
    void registerConnectionWatchdog(ConnectionWatchdog connectionWatchdog);

}
