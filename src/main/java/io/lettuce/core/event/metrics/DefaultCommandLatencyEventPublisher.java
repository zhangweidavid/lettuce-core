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
package io.lettuce.core.event.metrics;

import java.util.concurrent.TimeUnit;

import io.lettuce.core.event.EventBus;
import io.lettuce.core.event.EventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * 默认命令延迟事件发射器
 *
 */
public class DefaultCommandLatencyEventPublisher implements MetricEventPublisher {

    private final EventExecutorGroup eventExecutorGroup;
    private final EventPublisherOptions options;
    //事件总线
    private final EventBus eventBus;
    //命令延迟收集器
    private final CommandLatencyCollector commandLatencyCollector;
    //发射器
    private final Runnable EMITTER = this::emitMetricsEvent;

    private volatile ScheduledFuture<?> scheduledFuture;

    public DefaultCommandLatencyEventPublisher(EventExecutorGroup eventExecutorGroup, EventPublisherOptions options,
            EventBus eventBus, CommandLatencyCollector commandLatencyCollector) {

        this.eventExecutorGroup = eventExecutorGroup;
        this.options = options;
        this.eventBus = eventBus;
        this.commandLatencyCollector = commandLatencyCollector;
        //事件发射间隔不为0
        if (!options.eventEmitInterval().isZero()) {
            //固定间隔发送指标事件
            scheduledFuture = this.eventExecutorGroup.scheduleAtFixedRate(EMITTER, options.eventEmitInterval().toMillis(),
                    options.eventEmitInterval().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public boolean isEnabled() {
        //指标间隔不为0
        return !options.eventEmitInterval().isZero() && scheduledFuture != null;
    }

    @Override
    public void shutdown() {

        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }
    }

    @Override
    public void emitMetricsEvent() {

        if (!isEnabled() || !commandLatencyCollector.isEnabled()) {
            return;
        }
        //发送度量
        eventBus.publish(new CommandLatencyEvent(commandLatencyCollector.retrieveMetrics()));
    }

}
