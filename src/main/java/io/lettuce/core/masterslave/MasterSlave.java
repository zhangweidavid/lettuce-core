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

import java.util.*;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceLists;
import io.lettuce.core.models.role.RedisInstance;
import io.lettuce.core.models.role.RedisNodeDescription;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * 主备连接API
 * <p>
 * 该API允许连接到以静态主备设置或哨兵管理的运行的Redis主节点或备节点
 * 主备连接可以发现拓扑和为读操作使用ReadFrom选取一个资源
 * </p>
 * <p>
 *
 * Connections can be obtained by providing the {@link RedisClient}, a {@link RedisURI} and a {@link RedisCodec}.
 *
 * <pre class="code">
 * RedisClient client = RedisClient.create();
 * StatefulRedisMasterSlaveConnection&lt;String, String&gt; connection = MasterSlave.connect(client,
 *         RedisURI.create(&quot;redis://localhost&quot;), StringCodec.UTF8);
 * // ...
 *
 * connection.close();
 * client.shutdown();
 * </pre>
 *
 * </p>
 * <h3>Topology Discovery</h3>
 * <p>
 * Master-Slave topologies are either static or semi-static. Redis Standalone instances with attached slaves provide no
 * failover/HA mechanism. Redis Sentinel managed instances are controlled by Redis Sentinel and allow failover (which include
 * master promotion). The {@link MasterSlave} API supports both mechanisms. The topology is provided by a
 * {@link TopologyProvider}:
 *
 * <ul>
 * <li>{@link MasterSlaveTopologyProvider}: Dynamic topology lookup using the {@code INFO REPLICATION} output. Slaves are listed
 * as {@code slaveN=...} entries. The initial connection can either point to a master or a slave and the topology provider will
 * discover nodes. The connection needs to be re-established outside of lettuce in a case of Master/Slave failover or topology
 * changes.</li>
 * <li>{@link StaticMasterSlaveTopologyProvider}: Topology is defined by the list of {@link RedisURI URIs} and the {@code ROLE}
 * output. MasterSlave uses only the supplied nodes and won't discover additional nodes in the setup. The connection needs to be
 * re-established outside of lettuce in a case of Master/Slave failover or topology changes.</li>
 * <li>{@link SentinelTopologyProvider}: Dynamic topology lookup using the Redis Sentinel API. In particular,
 * {@code SENTINEL MASTER} and {@code SENTINEL SLAVES} output. Master/Slave failover is handled by lettuce.</li>
 * </ul>
 *
 * <p>
 * Topology Updates
 * </p>
 * <ul>
 * <li>Standalone Master/Slave: Performs a one-time topology lookup which remains static afterward</li>
 * <li>Redis Sentinel: Subscribes to all Sentinels and listens for Pub/Sub messages to trigger topology refreshing</li>
 * </ul>
 * </p>
 *
 * @author Mark Paluch
 * @since 4.1
 */
public class MasterSlave {

    private static final InternalLogger LOG = InternalLoggerFactory.getInstance(MasterSlave.class);

    /**
     * Open a new connection to a Redis Master-Slave server/servers using the supplied {@link RedisURI} and the supplied
     * {@link RedisCodec codec} to encode/decode keys.
     * <p>
     * This {@link MasterSlave} performs auto-discovery of nodes using either Redis Sentinel or Master/Slave. A {@link RedisURI}
     * can point to either a master or a slave host.
     * </p>
     *
     * @param redisClient the Redis client
     * @param codec Use this codec to encode/decode keys and values, must not be {@literal null}
     * @param redisURI the Redis server to connect to, must not be {@literal null}
     * @param <K> Key type
     * @param <V> Value type
     * @return A new connection
     */
    public static <K, V> StatefulRedisMasterSlaveConnection<K, V> connect(RedisClient redisClient, RedisCodec<K, V> codec,
            RedisURI redisURI) {

        LettuceAssert.notNull(redisClient, "RedisClient must not be null");
        LettuceAssert.notNull(codec, "RedisCodec must not be null");
        LettuceAssert.notNull(redisURI, "RedisURI must not be null");
        //如果是sentinel模式
        if (isSentinel(redisURI)) {
            return connectSentinel(redisClient, codec, redisURI);
        } else {//不是哨兵模式则使用静态主备方式建立连接
            return connectMasterSlave(redisClient, codec, redisURI);
        }
    }

    /**
     * Open a new connection to a Redis Master-Slave server/servers using the supplied {@link RedisURI} and the supplied
     * {@link RedisCodec codec} to encode/decode keys.
     * <p>
     * This {@link MasterSlave} performs auto-discovery of nodes if the URI is a Redis Sentinel URI. Master/Slave URIs will be
     * treated as static topology and no additional hosts are discovered in such case. Redis Standalone Master/Slave will
     * discover the roles of the supplied {@link RedisURI URIs} and issue commands to the appropriate node.
     * </p>
     *
     * @param redisClient the Redis client
     * @param codec Use this codec to encode/decode keys and values, must not be {@literal null}
     * @param redisURIs the Redis server to connect to, must not be {@literal null}
     * @param <K> Key type
     * @param <V> Value type
     * @return A new connection
     */
    public static <K, V> StatefulRedisMasterSlaveConnection<K, V> connect(RedisClient redisClient, RedisCodec<K, V> codec,
            Iterable<RedisURI> redisURIs) {

        LettuceAssert.notNull(redisClient, "RedisClient must not be null");
        LettuceAssert.notNull(codec, "RedisCodec must not be null");
        LettuceAssert.notNull(redisURIs, "RedisURIs must not be null");

        List<RedisURI> uriList = LettuceLists.newList(redisURIs);
        LettuceAssert.isTrue(!uriList.isEmpty(), "RedisURIs must not be empty");

        if (isSentinel(uriList.get(0))) {
            return connectSentinel(redisClient, codec, uriList.get(0));
        } else {
            return connectStaticMasterSlave(redisClient, codec, uriList);
        }
    }

    private static <K, V> StatefulRedisMasterSlaveConnection<K, V> connectSentinel(RedisClient redisClient,
            RedisCodec<K, V> codec, RedisURI redisURI) {
        //创建拓扑提供者为哨兵拓扑
        TopologyProvider topologyProvider = new SentinelTopologyProvider(redisURI.getSentinelMasterId(), redisClient, redisURI);

        //创建哨兵拓扑刷新服务
        SentinelTopologyRefresh sentinelTopologyRefresh = new SentinelTopologyRefresh(redisClient,
                redisURI.getSentinelMasterId(), redisURI.getSentinels());

        //利用拓扑提供者和redisClient创建主备拓扑刷新服务
        MasterSlaveTopologyRefresh refresh = new MasterSlaveTopologyRefresh(redisClient, topologyProvider);

        //创建主备连接提供者
        MasterSlaveConnectionProvider<K, V> connectionProvider = new MasterSlaveConnectionProvider<>(redisClient, codec,
                redisURI, Collections.emptyMap());
        //使用主备拓扑刷新服务获取所有节点将其设置到连接提供者中
        connectionProvider.setKnownNodes(refresh.getNodes(redisURI));

        //使用连接提供者创建主备通道写入器
        MasterSlaveChannelWriter<K, V> channelWriter = new MasterSlaveChannelWriter<>(connectionProvider);

        //创建连接
        StatefulRedisMasterSlaveConnectionImpl<K, V> connection = new StatefulRedisMasterSlaveConnectionImpl<>(channelWriter,
                codec, redisURI.getTimeout());

        connection.setOptions(redisClient.getOptions());

        Runnable runnable = () -> {
            try {

                LOG.debug("Refreshing topology");
                List<RedisNodeDescription> nodes = refresh.getNodes(redisURI);

                if (nodes.isEmpty()) {
                    LOG.warn("Topology refresh returned no nodes from {}", redisURI);
                }

                LOG.debug("New topology: {}", nodes);
                connectionProvider.setKnownNodes(nodes);
            } catch (Exception e) {
                LOG.error("Error during background refresh", e);
            }
        };

        try {
            //向连接注册可关闭服务
            connection.registerCloseables(new ArrayList<>(), sentinelTopologyRefresh);
            //绑定哨兵拓扑结构变化执行逻辑
            sentinelTopologyRefresh.bind(runnable);
        } catch (RuntimeException e) {

            connection.close();
            throw e;
        }

        return connection;
    }

    private static <K, V> StatefulRedisMasterSlaveConnection<K, V> connectMasterSlave(RedisClient redisClient,
            RedisCodec<K, V> codec, RedisURI redisURI) {
        //初始化连接
        Map<RedisURI, StatefulRedisConnection<K, V>> initialConnections = new HashMap<>();

        try {
            //创建连接
            StatefulRedisConnection<K, V> nodeConnection = redisClient.connect(codec, redisURI);
            //添加到初始化连接中
            initialConnections.put(redisURI, nodeConnection);
            //创建主备拓扑提供者
            TopologyProvider topologyProvider = new MasterSlaveTopologyProvider(nodeConnection, redisURI);
            //从主备拓扑提供者中获取已知节点
            List<RedisNodeDescription> nodes = topologyProvider.getNodes();
            //获取当前redisURI的node
            RedisNodeDescription node = getConnectedNode(redisURI, nodes);
             //如果这个节点不是master
            if (node.getRole() != RedisInstance.Role.MASTER) {
                //查找master节点
                RedisNodeDescription master = lookupMaster(nodes);
                //连接到master节点
                nodeConnection = redisClient.connect(codec, master.getUri());
                //添加到初始化连接池中
                initialConnections.put(master.getUri(), nodeConnection);
                //针对主节点创建拓扑提供器
                topologyProvider = new MasterSlaveTopologyProvider(nodeConnection, master.getUri());
            }
            //创建主备节点刷新服务
            MasterSlaveTopologyRefresh refresh = new MasterSlaveTopologyRefresh(redisClient, topologyProvider);
            MasterSlaveConnectionProvider<K, V> connectionProvider = new MasterSlaveConnectionProvider<>(redisClient, codec,
                    redisURI, initialConnections);
            //从拓扑刷新器中获取指定URI的节点
            connectionProvider.setKnownNodes(refresh.getNodes(redisURI));
            //创建主备通道写入器
            MasterSlaveChannelWriter<K, V> channelWriter = new MasterSlaveChannelWriter<>(connectionProvider);

            //创建主备连接器
            StatefulRedisMasterSlaveConnectionImpl<K, V> connection = new StatefulRedisMasterSlaveConnectionImpl<>(
                    channelWriter, codec, redisURI.getTimeout());

            connection.setOptions(redisClient.getOptions());

            return connection;

        } catch (RuntimeException e) {
            for (StatefulRedisConnection<K, V> connection : initialConnections.values()) {
                connection.close();
            }
            throw e;
        }
    }

    private static <K, V> StatefulRedisMasterSlaveConnection<K, V> connectStaticMasterSlave(RedisClient redisClient,
            RedisCodec<K, V> codec, Iterable<RedisURI> redisURIs) {

        Map<RedisURI, StatefulRedisConnection<K, V>> initialConnections = new HashMap<>();

        try {
            TopologyProvider topologyProvider = new StaticMasterSlaveTopologyProvider(redisClient, redisURIs);

            RedisURI seedNode = redisURIs.iterator().next();

            MasterSlaveTopologyRefresh refresh = new MasterSlaveTopologyRefresh(redisClient, topologyProvider);
            MasterSlaveConnectionProvider<K, V> connectionProvider = new MasterSlaveConnectionProvider<>(redisClient, codec,
                    seedNode, initialConnections);

            List<RedisNodeDescription> nodes = refresh.getNodes(seedNode);
            if (nodes.isEmpty()) {
                throw new RedisException(String.format("Cannot determine topology from %s", redisURIs));
            }

            connectionProvider.setKnownNodes(nodes);

            MasterSlaveChannelWriter<K, V> channelWriter = new MasterSlaveChannelWriter<>(connectionProvider);

            StatefulRedisMasterSlaveConnectionImpl<K, V> connection = new StatefulRedisMasterSlaveConnectionImpl<>(
                    channelWriter, codec, seedNode.getTimeout());

            connection.setOptions(redisClient.getOptions());

            return connection;

        } catch (RuntimeException e) {
            for (StatefulRedisConnection<K, V> connection : initialConnections.values()) {
                connection.close();
            }
            throw e;
        }
    }

    private static RedisNodeDescription lookupMaster(List<RedisNodeDescription> nodes) {

        Optional<RedisNodeDescription> first = nodes.stream().filter(n -> n.getRole() == RedisInstance.Role.MASTER).findFirst();
        return first.orElseThrow(() -> new IllegalStateException("Cannot lookup master from " + nodes));
    }

    private static RedisNodeDescription getConnectedNode(RedisURI redisURI, List<RedisNodeDescription> nodes) {

        Optional<RedisNodeDescription> first = nodes.stream().filter(n -> equals(redisURI, n)).findFirst();
        return first.orElseThrow(() -> new IllegalStateException("Cannot lookup node descriptor for connected node at "
                + redisURI));
    }

    private static boolean equals(RedisURI redisURI, RedisNodeDescription node) {
        return node.getUri().getHost().equals(redisURI.getHost()) && node.getUri().getPort() == redisURI.getPort();
    }

    private static boolean isSentinel(RedisURI redisURI) {
        return !redisURI.getSentinels().isEmpty();
    }

}
