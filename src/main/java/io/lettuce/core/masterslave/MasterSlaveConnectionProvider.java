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

import static io.lettuce.core.masterslave.MasterSlaveUtils.findNodeByHostAndPort;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceSets;
import io.lettuce.core.models.role.RedisInstance;
import io.lettuce.core.models.role.RedisNodeDescription;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * 主从连接的提供者
 * @author Mark Paluch
 * @since 4.1
 */
public class MasterSlaveConnectionProvider<K, V> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MasterSlaveConnectionProvider.class);
    private final boolean debugEnabled = logger.isDebugEnabled();

    //连接缓存
    private final Map<ConnectionKey, StatefulRedisConnection<K, V>> connections = new ConcurrentHashMap<>();
    //连接工厂
    private final ConnectionFactory<K, V> connectionFactory;
    //初始化的RedisURI
    private final RedisURI initialRedisUri;
    //已知节点
    private List<RedisNodeDescription> knownNodes = new ArrayList<>();
    //是否自动刷新命令
    private boolean autoFlushCommands = true;
    //状态锁
    private final Object stateLock = new Object();
    //路由选择器
    private ReadFrom readFrom;

    MasterSlaveConnectionProvider(RedisClient redisClient, RedisCodec<K, V> redisCodec, RedisURI initialRedisUri,
            Map<RedisURI, StatefulRedisConnection<K, V>> initialConnections) {

        this.initialRedisUri = initialRedisUri;
        //初始化连接工厂
        this.connectionFactory = new ConnectionFactory<>(redisClient, redisCodec);
        //遍历初始化连接
        for (Map.Entry<RedisURI, StatefulRedisConnection<K, V>> entry : initialConnections.entrySet()) {
            connections.put(toConnectionKey(entry.getKey()), entry.getValue());
        }
    }


    //根据意图获取连接，该方法主要实现了路由选择功能
    public StatefulRedisConnection<K, V> getConnection(Intent intent) {

        if (debugEnabled) {
            logger.debug("getConnection(" + intent + ")");
        }
        //如果readFrom不为null且是READ
        if (readFrom != null && intent == Intent.READ) {
            //根据readFrom策略从已知节点中选择可用节点列表
            List<RedisNodeDescription> selection = readFrom.select(new ReadFrom.Nodes() {
                @Override
                public List<RedisNodeDescription> getNodes() {
                    return knownNodes;
                }

                @Override
                public Iterator<RedisNodeDescription> iterator() {
                    return knownNodes.iterator();
                }
            });
            //如果readFrom选择的可用列表为空则抛出异常，一个请求最终必须路由到这个节点
            if (selection.isEmpty()) {
                throw new RedisException(String.format("Cannot determine a node to read (Known nodes: %s) with setting %s",
                        knownNodes, readFrom));
            }
            try {
                //遍历readFrom策略选择出的可用节点列表选贼一个可用的节点；这个按顺序选择返回第一个可用节点；根据该特点readFrom可以有不同的策略
                for (RedisNodeDescription redisNodeDescription : selection) {
                    //获取节点连接
                    StatefulRedisConnection<K, V> readerCandidate = getConnection(redisNodeDescription);
                    //如果节点连接不是打开到连接则继续查找下一个连接
                    if (!readerCandidate.isOpen()) {
                        continue;
                    }
                    //返回可用连接
                    return readerCandidate;
                }
                //如果没有找到可用连接，默认返回第一个
                return getConnection(selection.get(0));
            } catch (RuntimeException e) {
                throw new RedisException(e);
            }
        }
        //如果没有配置readFrom或者不是READ 则返回master连接
        return getConnection(getMaster());
    }

    protected StatefulRedisConnection<K, V> getConnection(RedisNodeDescription redisNodeDescription) {
       //如果没有则创建新节点，并添加到缓存中，computeIfAbsent是1.8新增方法，如果map中没有则通过映射方法创建一个新对象保存到map中
        return connections.computeIfAbsent(
                new ConnectionKey(redisNodeDescription.getUri().getHost(), redisNodeDescription.getUri().getPort()),
                connectionFactory);
    }

    /**
     *
     * @return number of connections.
     */
    protected long getConnectionCount() {
        return connections.size();
    }

    /**
     * Retrieve a set of PoolKey's for all pooled connections that are within the pool but not within the {@link Partitions}.
     *
     * @return Set of {@link ConnectionKey}s
     */
    private Set<ConnectionKey> getStaleConnectionKeys() {
        //创建一个新的map
        Map<ConnectionKey, StatefulRedisConnection<K, V>> map = new HashMap<>(connections);
        //陈旧缓存key集合
        Set<ConnectionKey> stale = new HashSet<>();
        //遍历所有缓存的连接key
        for (ConnectionKey connectionKey : map.keySet()) {
              //如果 key中的host不为null且已知节点中存在该host,port则继续
            if (connectionKey.host != null
                    && findNodeByHostAndPort(knownNodes, connectionKey.host, connectionKey.port) != null) {
                continue;
            }
            //如果不存在则表示该缓存是一个陈旧的connection
            stale.add(connectionKey);
        }
        return stale;
    }

    /**
     * Close stale connections.
     */
    public void closeStaleConnections() {
        logger.debug("closeStaleConnections() count before expiring: {}", getConnectionCount());
        //获取失效的连接
        Set<ConnectionKey> stale = getStaleConnectionKeys();
        //遍历所有失效的连接Key
        for (ConnectionKey connectionKey : stale) {
            //获取失效连接
            StatefulRedisConnection<K, V> connection = connections.get(connectionKey);
            //如果连接不为null,则将连接从缓存池中删除，并关闭连接
            if (connection != null) {
                connections.remove(connectionKey);
                connection.close();
            }
        }

        logger.debug("closeStaleConnections() count after expiring: {}", getConnectionCount());
    }

    public void reset() {
        allConnections().forEach(StatefulRedisConnection::reset);
    }

    /**
     * Close all connections.
     */
    public void close() {

        Collection<StatefulRedisConnection<K, V>> connections = allConnections();
        this.connections.clear();
        connections.forEach(StatefulRedisConnection::close);
    }

    public void flushCommands() {
        allConnections().forEach(StatefulConnection::flushCommands);
    }

    public void setAutoFlushCommands(boolean autoFlushCommands) {
        synchronized (stateLock) {
        }
        allConnections().forEach(connection -> connection.setAutoFlushCommands(autoFlushCommands));
    }

    protected Collection<StatefulRedisConnection<K, V>> allConnections() {

        Set<StatefulRedisConnection<K, V>> connections = LettuceSets.newHashSet(this.connections.values());
        return (Collection) connections;
    }

    /**
     *
     * @param knownNodes
     */
    public void setKnownNodes(Collection<RedisNodeDescription> knownNodes) {
        //通过stateLock加同步锁
        synchronized (stateLock) {
            //清空已知节点
            this.knownNodes.clear();
            //添加新的节点
            this.knownNodes.addAll(knownNodes);
            //关闭过期连接，在清除之前节点和当前节点之间可能存在差异，可能出现部分节点失联所以需要清除
            closeStaleConnections();
        }
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(ReadFrom readFrom) {
        synchronized (stateLock) {
            this.readFrom = readFrom;
        }
    }

    public RedisNodeDescription getMaster() {
        for (RedisNodeDescription knownNode : knownNodes) {
            if (knownNode.getRole() == RedisInstance.Role.MASTER) {
                return knownNode;
            }
        }

        throw new RedisException(String.format("Master is currently unknown: %s", knownNodes));
    }

    /**
     * 连接工厂
     */
    private class ConnectionFactory<K, V> implements Function<ConnectionKey, StatefulRedisConnection<K, V>> {

        private final RedisClient redisClient;
        private final RedisCodec<K, V> redisCodec;

        public ConnectionFactory(RedisClient redisClient, RedisCodec<K, V> redisCodec) {
            this.redisClient = redisClient;
            this.redisCodec = redisCodec;
        }

        @Override
        public StatefulRedisConnection<K, V> apply(ConnectionKey key) {
            //构建URI
            RedisURI.Builder builder = RedisURI.Builder
                    .redis(key.host, key.port)
                    .withSsl(initialRedisUri.isSsl())
                    .withVerifyPeer(initialRedisUri.isVerifyPeer())
                    .withStartTls(initialRedisUri.isStartTls());

            if (initialRedisUri.getPassword() != null && initialRedisUri.getPassword().length != 0) {
                builder.withPassword(initialRedisUri.getPassword());
            }

            if (initialRedisUri.getClientName() != null) {
                builder.withClientName(initialRedisUri.getClientName());
            }
            builder.withDatabase(initialRedisUri.getDatabase());

            //创建连接
            StatefulRedisConnection<K, V> connection = redisClient.connect(redisCodec, builder.build());

            //设置是否自动提交
            synchronized (stateLock) {
                connection.setAutoFlushCommands(autoFlushCommands);
            }

            return connection;
        }
    }

    private ConnectionKey toConnectionKey(RedisURI redisURI) {
        return new ConnectionKey(redisURI.getHost(), redisURI.getPort());
    }

    /**
     * Connection to identify a connection by host/port.
     */
    private static class ConnectionKey {
        private final String host;
        private final int port;

        public ConnectionKey(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof ConnectionKey))
                return false;

            ConnectionKey that = (ConnectionKey) o;

            if (port != that.port)
                return false;
            return !(host != null ? !host.equals(that.host) : that.host != null);

        }

        @Override
        public int hashCode() {
            int result = (host != null ? host.hashCode() : 0);
            result = 31 * result + port;
            return result;
        }
    }

    enum Intent {
        READ, WRITE;
    }
}
