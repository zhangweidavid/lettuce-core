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
package io.lettuce.core.support;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.pool2.impl.SoftReferenceObjectPool;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.internal.AbstractInvocationHandler;
import io.lettuce.core.internal.LettuceAssert;

/**
 *  支持GenericObjectPool和SoftReferenceObjectPool的连接池。连接池创建要求有一个创建Redis连接的提供者。连接池可以分配，包装或跳转连接
 * Connection pool support for {@link GenericObjectPool} and {@link SoftReferenceObjectPool}. Connection pool creation requires
 * a {@link Supplier} that creates Redis connections. The pool can allocate either wrapped or direct connections.
 * <ul>
 * <li>Wrapped instances will return the connection back to the pool when called {@link StatefulConnection#close()}.</li>
 * <li>Regular connections need to be returned to the pool with {@link GenericObjectPool#returnObject(Object)}</li>
 * </ul>
 * <p>
 * Lettuce connections are designed to be thread-safe so one connection can be shared amongst multiple threads and lettuce
 * connections {@link ClientOptions#isAutoReconnect() auto-reconnect} by default. Connection pooling with lettuce might be
 * required when you're invoking Redis operations in multiple threads and you use
 * <ul>
 * <li>blocking commands such as {@code BLPOP}.</li>
 * <li>transactions {@code BLPOP}.</li>
 * <li>{@link StatefulConnection#setAutoFlushCommands(boolean) command batching}.</li>
 * </ul>
 *
 * Transactions and command batching affect connection state. Blocking commands won't propagate queued commands to Redis until
 * the blocking command is resolved.
 *
 * <h3>Example</h3>
 *
 * <pre class="code">
 * // application initialization
 * RedisClusterClient clusterClient = RedisClusterClient.create(RedisURI.create(host, port));
 * GenericObjectPool&lt;StatefulRedisClusterConnection&lt;String, String&gt;&gt; pool = ConnectionPoolSupport.createGenericObjectPool(
 *         () -&gt; clusterClient.connect(), new GenericObjectPoolConfig());
 *
 * // executing work
 * try (StatefulRedisClusterConnection&lt;String, String&gt; connection = pool.borrowObject()) {
 *     // perform some work
 * }
 *
 * // terminating
 * pool.close();
 * clusterClient.shutdown();
 * </pre>
 *
 * @author Mark Paluch
 * @since 4.3
 */
public abstract class ConnectionPoolSupport {

    private ConnectionPoolSupport() {
    }

    /**
     * Creates a new {@link GenericObjectPool} using the {@link Supplier}. Allocated instances are wrapped and must not be
     * returned with {@link ObjectPool#returnObject(Object)}.
     *
     * @param connectionSupplier must not be {@literal null}.
     * @param config must not be {@literal null}.
     * @param <T> connection type.
     * @return the connection pool.
     */
    public static <T extends StatefulConnection<?, ?>> GenericObjectPool<T> createGenericObjectPool(
            Supplier<T> connectionSupplier, GenericObjectPoolConfig config) {
        return createGenericObjectPool(connectionSupplier, config, true);
    }

    /**
     * Creates a new {@link GenericObjectPool} using the {@link Supplier}.
     *
     * @param connectionSupplier must not be {@literal null}.
     * @param config must not be {@literal null}.
     * @param wrapConnections {@literal false} to return direct connections that need to be returned to the pool using
     *        {@link ObjectPool#returnObject(Object)}. {@literal true} to return wrapped connection that are returned to the
     *        pool when invoking {@link StatefulConnection#close()}.
     * @param <T> connection type.
     * @return the connection pool.
     */
    @SuppressWarnings("unchecked")
    public static <T extends StatefulConnection<?, ?>> GenericObjectPool<T> createGenericObjectPool(
            Supplier<T> connectionSupplier, GenericObjectPoolConfig config, boolean wrapConnections) {

        LettuceAssert.notNull(connectionSupplier, "Connection supplier must not be null");
        LettuceAssert.notNull(config, "GenericObjectPoolConfig must not be null");

        AtomicReference<ObjectPool<T>> poolRef = new AtomicReference<>();

        GenericObjectPool<T> pool = new GenericObjectPool<T>(new RedisPooledObjectFactory<T>(connectionSupplier), config) {

            @Override
            public T borrowObject() throws Exception {
                //如果wrapConnection 设置为true,则对连接创建动态代理
                return wrapConnections ? wrapConnection(super.borrowObject(), this) : super.borrowObject();
            }

            @Override
            public void returnObject(T obj) {

                if (wrapConnections && obj instanceof HasTargetConnection) {
                    super.returnObject((T) ((HasTargetConnection) obj).getTargetConnection());
                    return;
                }
                super.returnObject(obj);
            }
        };

        poolRef.set(pool);

        return pool;
    }

    /**
     * Creates a new {@link SoftReferenceObjectPool} using the {@link Supplier}. Allocated instances are wrapped and must not be
     * returned with {@link ObjectPool#returnObject(Object)}.
     *
     * @param connectionSupplier must not be {@literal null}.
     * @param <T> connection type.
     * @return the connection pool.
     */
    public static <T extends StatefulConnection<?, ?>> SoftReferenceObjectPool<T> createSoftReferenceObjectPool(
            Supplier<T> connectionSupplier) {
        return createSoftReferenceObjectPool(connectionSupplier, true);
    }

    /**
     * Creates a new {@link SoftReferenceObjectPool} using the {@link Supplier}.
     *
     * @param connectionSupplier must not be {@literal null}.
     * @param wrapConnections {@literal false} to return direct connections that need to be returned to the pool using
     *        {@link ObjectPool#returnObject(Object)}. {@literal true} to return wrapped connection that are returned to the
     *        pool when invoking {@link StatefulConnection#close()}.
     * @param <T> connection type.
     * @return the connection pool.
     */
    @SuppressWarnings("unchecked")
    public static <T extends StatefulConnection<?, ?>> SoftReferenceObjectPool<T> createSoftReferenceObjectPool(
            Supplier<T> connectionSupplier, boolean wrapConnections) {

        LettuceAssert.notNull(connectionSupplier, "Connection supplier must not be null");

        AtomicReference<ObjectPool<T>> poolRef = new AtomicReference<>();

        SoftReferenceObjectPool<T> pool = new SoftReferenceObjectPool<T>(new RedisPooledObjectFactory<>(connectionSupplier)) {
            @Override
            public T borrowObject() throws Exception {
                return wrapConnections ? wrapConnection(super.borrowObject(), this) : super.borrowObject();
            }

            @Override
            public void returnObject(T obj) throws Exception {

                if (wrapConnections && obj instanceof HasTargetConnection) {
                    super.returnObject((T) ((HasTargetConnection) obj).getTargetConnection());
                    return;
                }
                super.returnObject(obj);
            }
        };
        poolRef.set(pool);

        return pool;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static <T> T wrapConnection(T connection, ObjectPool<T> pool) {

        //创建调用处理器
        ReturnObjectOnCloseInvocationHandler<T> handler = new ReturnObjectOnCloseInvocationHandler<T>(connection, pool);

        Class<?>[] implementedInterfaces = connection.getClass().getInterfaces();
        Class[] interfaces = new Class[implementedInterfaces.length + 1];
        interfaces[0] = HasTargetConnection.class;
        System.arraycopy(implementedInterfaces, 0, interfaces, 1, implementedInterfaces.length);
        //创建代理连接
        T proxiedConnection = (T) Proxy.newProxyInstance(connection.getClass().getClassLoader(), interfaces, handler);
        //向连接调用处理器设置代理连接
        handler.setProxiedConnection(proxiedConnection);
        //返回代理连接
        return proxiedConnection;
    }

    /**
     * @author Mark Paluch
     * @since 4.3
     */
    private static class RedisPooledObjectFactory<T extends StatefulConnection<?, ?>> extends BasePooledObjectFactory<T> {

        private final Supplier<T> connectionSupplier;

        RedisPooledObjectFactory(Supplier<T> connectionSupplier) {
            this.connectionSupplier = connectionSupplier;
        }

        @Override
        public T create() throws Exception {
            return connectionSupplier.get();
        }

        @Override
        public void destroyObject(PooledObject<T> p) throws Exception {
            p.getObject().close();
        }

        @Override
        public PooledObject<T> wrap(T obj) {
            return new DefaultPooledObject<>(obj);
        }

        @Override
        public boolean validateObject(PooledObject<T> p) {
            return p.getObject().isOpen();
        }
    }

    /**
     * Invocation handler that takes care of connection.close(). Connections are returned to the pool on a close()-call.
     *
     * @author Mark Paluch
     * @param <T> Connection type.
     * @since 4.3
     */
    private static class ReturnObjectOnCloseInvocationHandler<T> extends AbstractInvocationHandler {
        //被代理对连接
        private T connection;
        private T proxiedConnection;
        private Map<Method, Object> connectionProxies = new ConcurrentHashMap<>(5, 1);
        //连接池
        private final ObjectPool<T> pool;

        ReturnObjectOnCloseInvocationHandler(T connection, ObjectPool<T> pool) {
            this.connection = connection;
            this.pool = pool;
        }

        //设置代理连接
        void setProxiedConnection(T proxiedConnection) {
            this.proxiedConnection = proxiedConnection;
        }

        @Override
        protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
             //如果调用方法是  getStatefulConnection则返回代理连接
            if (method.getName().equals("getStatefulConnection")) {
                return proxiedConnection;
            }
            //如果调用的方法是getTargetConnection 则返回真实连接
            if (method.getName().equals("getTargetConnection")) {
                return connection;
            }
            //如果真实连接为null则抛出异常
            if (connection == null) {
                throw new RedisException("Connection is deallocated and cannot be used anymore.");
            }
            //如果调用的方法是close则将代理连接归还到连接池，并将真实连接设置和代理连接设置为null
            if (method.getName().equals("close")) {
                pool.returnObject(proxiedConnection);
                connection = null;
                proxiedConnection = null;
                connectionProxies.clear();
                return null;
            }

            try {
                //如果调用方法是获取连接则从代理连接池中获取，如果没有则创建代理连接并放入缓存
                if (method.getName().equals("sync") || method.getName().equals("async") || method.getName().equals("reactive")) {
                    return connectionProxies.computeIfAbsent(
                            method, m -> getInnerProxy(method, args));
                }
                //其它方法不在多任何拦截
                return method.invoke(connection, args);

            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }

        @SuppressWarnings("unchecked")
        private Object getInnerProxy(Method method, Object[] args) {

            try {
                Object result = method.invoke(connection, args);

                result = Proxy.newProxyInstance(getClass().getClassLoader(), result.getClass().getInterfaces(),
                        new DelegateCloseToConnectionInvocationHandler<>((AutoCloseable) proxiedConnection, result));

                return result;
            } catch (IllegalAccessException e) {
                throw new RedisException(e);
            } catch (InvocationTargetException e) {
                throw new RedisException(e.getTargetException());

            }
        }

        public T getConnection() {
            return connection;
        }
    }

    /**
     * Invocation handler that takes care of connection.close(). Connections are returned to the pool on a close()-call.
     *
     * @author Mark Paluch
     * @param <T> Connection type.
     * @since 4.3
     */
    private static class DelegateCloseToConnectionInvocationHandler<T extends AutoCloseable> extends AbstractInvocationHandler {

        private final T proxiedConnection;
        private final Object api;

        DelegateCloseToConnectionInvocationHandler(T proxiedConnection, Object api) {

            this.proxiedConnection = proxiedConnection;
            this.api = api;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {

            if (method.getName().equals("getStatefulConnection")) {
                return proxiedConnection;
            }

            try {

                if (method.getName().equals("close")) {
                    proxiedConnection.close();
                    return null;
                }

                return method.invoke(api, args);

            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }

    /**
     * Interface to retrieve an underlying target connection from a proxy.
     */
    interface HasTargetConnection {
        StatefulConnection<?, ?> getTargetConnection();
    }
}
