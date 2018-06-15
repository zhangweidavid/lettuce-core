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
package io.lettuce.core.masterslave;

import static io.lettuce.core.masterslave.MasterSlaveUtils.CODEC;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.models.role.RedisInstance;
import io.lettuce.core.models.role.RedisNodeDescription;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Topology provider using Redis Sentinel and the Sentinel API.
 *
 * @author Mark Paluch
 * @since 4.1
 */
public class SentinelTopologyProvider implements TopologyProvider {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SentinelTopologyProvider.class);
    //主节点ID
    private final String masterId;
    //redis客户端
    private final RedisClient redisClient;
    //哨兵
    private final RedisURI sentinelUri;
    //超时时间
    private final Duration timeout;

    /**
     * Creates a new {@link SentinelTopologyProvider}.
     *
     * @param masterId must not be empty
     * @param redisClient must not be {@literal null}.
     * @param sentinelUri must not be {@literal null}.
     */
    public SentinelTopologyProvider(String masterId, RedisClient redisClient, RedisURI sentinelUri) {

        LettuceAssert.notEmpty(masterId, "MasterId must not be empty");
        LettuceAssert.notNull(redisClient, "RedisClient must not be null");
        LettuceAssert.notNull(sentinelUri, "Sentinel URI must not be null");
        //主节点
        this.masterId = masterId;
        this.redisClient = redisClient;
        //sentinel URI
        this.sentinelUri = sentinelUri;
        //从uri中获取超时时间
        this.timeout = sentinelUri.getTimeout();
    }

    @Override
    public List<RedisNodeDescription> getNodes() {

        logger.debug("lookup topology for masterId {}", masterId);
        //自动关闭方式连接sentinel
        try (StatefulRedisSentinelConnection<String, String> connection = redisClient.connectSentinel(CODEC, sentinelUri)) {
            //异步获取主节点
            RedisFuture<Map<String, String>> masterFuture = connection.async().master(masterId);
            //异步获取slave节点
            RedisFuture<List<Map<String, String>>> slavesFuture = connection.async().slaves(masterId);

            List<RedisNodeDescription> result = new ArrayList<>();
            try {
                //获取主节点
                Map<String, String> master = masterFuture.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                //获取slave节点
                List<Map<String, String>> slaves = slavesFuture.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                //添加master节点
                result.add(toNode(master, RedisInstance.Role.MASTER));
                //添加所有slave节点
                result.addAll(slaves.stream().filter(SentinelTopologyProvider::isAvailable)
                        .map(map -> toNode(map, RedisInstance.Role.SLAVE)).collect(Collectors.toList()));

            } catch (ExecutionException | InterruptedException | TimeoutException e) {
                throw new RedisException(e);
            }

            return result;
        }
    }

    private static boolean isAvailable(Map<String, String> map) {
        //获取flags
        String flags = map.get("flags");
        //如果flags不为null
        if (flags != null) {
            //flags中存在s_down 或 o_down 或disconnected 则表示该节点不可用
            if (flags.contains("s_down") || flags.contains("o_down") || flags.contains("disconnected")) {
                return false;
            }
        }
        return true;
    }

    private RedisNodeDescription toNode(Map<String, String> map, RedisInstance.Role role) {

        String ip = map.get("ip");
        String port = map.get("port");
        return new RedisMasterSlaveNode(ip, Integer.parseInt(port), sentinelUri, role);
    }

}
