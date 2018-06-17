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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import io.lettuce.core.resource.ClientResources;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author Mark Paluch
 */
class ClusterTopologyRefreshScheduler implements Runnable, ClusterEventListener {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ClusterTopologyRefreshScheduler.class);
    private static final ClusterTopologyRefreshOptions FALLBACK_OPTIONS = ClusterTopologyRefreshOptions.create();

    private final RedisClusterClient redisClusterClient;
    private final ClientResources clientResources;
    private final ClusterTopologyRefreshTask clusterTopologyRefreshTask;
    //超时引用
    private final AtomicReference<Timeout> timeoutRef = new AtomicReference<>();

    ClusterTopologyRefreshScheduler(RedisClusterClient redisClusterClient, ClientResources clientResources) {

        this.redisClusterClient = redisClusterClient;
        this.clientResources = clientResources;
        this.clusterTopologyRefreshTask = new ClusterTopologyRefreshTask(redisClusterClient);
    }

    @Override
    public void run() {

        logger.debug("ClusterTopologyRefreshScheduler.run()");

        if (isEventLoopActive() && redisClusterClient.getClusterClientOptions() != null) {
            if (!redisClusterClient.getClusterClientOptions().isRefreshClusterView()) {
                logger.debug("Periodic ClusterTopologyRefresh is disabled");
                return;
            }
        } else {
            logger.debug("Periodic ClusterTopologyRefresh is disabled");
            return;
        }

        clientResources.eventExecutorGroup().submit(clusterTopologyRefreshTask);
    }

    private void indicateTopologyRefreshSignal() {

        logger.debug("ClusterTopologyRefreshScheduler.indicateTopologyRefreshSignal()");
        //如果没有超时则不再刷新
        if (!acquireTimeout()) {
            return;
        }
        //如果线程池有效且集群客户端选项不为null则提交刷新任务
        if (isEventLoopActive() && redisClusterClient.getClusterClientOptions() != null) {
            clientResources.eventExecutorGroup().submit(clusterTopologyRefreshTask);
        } else {
            logger.debug("Adaptive ClusterTopologyRefresh is disabled");
        }
    }

    /**
     * Check if the {@link EventExecutorGroup} is active
     *
     * @return false if the worker pool is terminating, shutdown or terminated
     */
    protected boolean isEventLoopActive() {

        EventExecutorGroup eventExecutors = clientResources.eventExecutorGroup();
        if (eventExecutors.isShuttingDown() || eventExecutors.isShutdown() || eventExecutors.isTerminated()) {
            return false;
        }

        return true;
    }

    private boolean acquireTimeout() {
        //获取已经存在的timeout
        Timeout existingTimeout = timeoutRef.get();
        //如果存在timeout且没有过期则返回false
        if (existingTimeout != null) {
            if (!existingTimeout.isExpired()) {
                return false;
            }
        }
        //当前引用不存在timeout或timeout已经过期，则根据拓扑刷新选项中配置的刷新超时时间构建timeout并CAS更新到引用
        ClusterTopologyRefreshOptions refreshOptions = getClusterTopologyRefreshOptions();
        Timeout timeout = new Timeout(refreshOptions.getAdaptiveRefreshTimeout());

        if (timeoutRef.compareAndSet(existingTimeout, timeout)) {
            return true;
        }

        return false;
    }

    @Override
    public void onAskRedirection() {
        //如果配置了ask时刷新集群拓扑则选择刷新拓扑接口信号
        if (isEnabled(ClusterTopologyRefreshOptions.RefreshTrigger.ASK_REDIRECT)) {
            indicateTopologyRefreshSignal();
        }
    }

    @Override
    public void onMovedRedirection() {

        if (isEnabled(ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT)) {
            indicateTopologyRefreshSignal();
        }
    }

    @Override
    public void onReconnection(int attempt) {

        if (isEnabled(ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
                && attempt >= getClusterTopologyRefreshOptions().getRefreshTriggersReconnectAttempts()) {
            indicateTopologyRefreshSignal();
        }
    }

    private ClusterTopologyRefreshOptions getClusterTopologyRefreshOptions() {

        ClusterClientOptions clusterClientOptions = redisClusterClient.getClusterClientOptions();

        if (clusterClientOptions != null) {
            return clusterClientOptions.getTopologyRefreshOptions();
        }

        return FALLBACK_OPTIONS;
    }

    private boolean isEnabled(ClusterTopologyRefreshOptions.RefreshTrigger refreshTrigger) {
        return getClusterTopologyRefreshOptions().getAdaptiveRefreshTriggers().contains(refreshTrigger);
    }

    /**
     * Value object to represent a timeout.
     *
     * @author Mark Paluch
     * @since 4.2
     */
    private class Timeout {

        private final long expiresMs;

        public Timeout(Duration duration) {
            this.expiresMs = System.currentTimeMillis() + duration.toMillis();
        }

        public boolean isExpired() {
            return expiresMs < System.currentTimeMillis();
        }

        public long remaining() {

            long diff = expiresMs - System.currentTimeMillis();
            if (diff > 0) {
                return diff;
            }
            return 0;
        }
    }

    private static class ClusterTopologyRefreshTask implements Runnable {

        private final RedisClusterClient redisClusterClient;

        public ClusterTopologyRefreshTask(RedisClusterClient redisClusterClient) {
            this.redisClusterClient = redisClusterClient;
        }

        public void run() {

            if (logger.isDebugEnabled()) {
                logger.debug("ClusterTopologyRefreshTask requesting partitions from {}",
                        redisClusterClient.getTopologyRefreshSource());
            }
            try {
                redisClusterClient.reloadPartitions();
            } catch (Exception e) {
                logger.warn("Cannot refresh Redis Cluster topology", e);
            }
        }
    }
}
