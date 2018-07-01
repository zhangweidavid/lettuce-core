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

import static io.lettuce.core.ConnectionEventTrigger.local;
import static io.lettuce.core.ConnectionEventTrigger.remote;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.lettuce.core.ConnectionEvents.PingBeforeActivate;
import io.lettuce.core.event.connection.ConnectedEvent;
import io.lettuce.core.event.connection.ConnectionActivatedEvent;
import io.lettuce.core.event.connection.DisconnectedEvent;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.resource.ClientResources;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Timeout;

/**
 * 通道
 * @author Mark Paluch
 */
class PlainChannelInitializer extends io.netty.channel.ChannelInitializer<Channel> implements RedisChannelInitializer {

    //不ping
    final static Supplier<AsyncCommand<?, ?, ?>> NO_PING = () -> null;
    //处理器提供器
    private final Supplier<List<ChannelHandler>> handlers;
    //ping命令提供器
    private final Supplier<AsyncCommand<?, ?, ?>> pingCommandSupplier;
    private final ClientResources clientResources;
    //超时时间
    private final Duration timeout;

    private volatile CompletableFuture<Boolean> initializedFuture = new CompletableFuture<>();

    PlainChannelInitializer(Supplier<AsyncCommand<?, ?, ?>> pingCommandSupplier, Supplier<List<ChannelHandler>> handlers,
            ClientResources clientResources, Duration timeout) {
        this.pingCommandSupplier = pingCommandSupplier;
        this.handlers = handlers;
        this.clientResources = clientResources;
        this.timeout = timeout;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {

        //如果pipeline中没有配置channelActivator则需要添加channelActivator处理器
        if (channel.pipeline().get("channelActivator") == null) {

            channel.pipeline().addLast("channelActivator", new RedisChannelInitializerImpl() {

                private AsyncCommand<?, ?, ?> pingCommand;

                @Override
                public CompletableFuture<Boolean> channelInitialized() {
                    return initializedFuture;
                }

                @Override
                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                    //如果通道断开连接
                    clientResources.eventBus().publish(new DisconnectedEvent(local(ctx), remote(ctx)));
                    //如果初始化没有完成则抛出异常
                    if (!initializedFuture.isDone()) {
                        initializedFuture.completeExceptionally(new RedisConnectionException("Connection closed prematurely"));
                    }

                    initializedFuture = new CompletableFuture<>();
                    pingCommand = null;
                    super.channelInactive(ctx);
                }

                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

                    if (evt instanceof ConnectionEvents.Activated) {
                        if (!initializedFuture.isDone()) {
                            initializedFuture.complete(true);
                            clientResources.eventBus().publish(new ConnectionActivatedEvent(local(ctx), remote(ctx)));
                        }
                    }
                    super.userEventTriggered(ctx, evt);
                }

                @Override
                public void channelActive(final ChannelHandlerContext ctx) throws Exception {
                    //通过事件总线发送连接事件
                    clientResources.eventBus().publish(new ConnectedEvent(local(ctx), remote(ctx)));
                    //如果ping命令提供器不是NO_PING则发送执行ping
                    if (pingCommandSupplier != NO_PING) {
                        pingCommand = pingCommandSupplier.get();
                        pingBeforeActivate(pingCommand, initializedFuture, ctx, clientResources, timeout);
                    } else {
                        super.channelActive(ctx);
                    }
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                    if (!initializedFuture.isDone()) {
                        initializedFuture.completeExceptionally(cause);
                    }
                    super.exceptionCaught(ctx, cause);
                }
            });
        }
        //将hanler提供器提供的的处理器添加到该频道的管道中
        for (ChannelHandler handler : handlers.get()) {
            channel.pipeline().addLast(handler);
        }
        //扩展点，用户可以对向pipline中添加自定义的channel
        clientResources.nettyCustomizer().afterChannelInitialized(channel);
    }

    static void pingBeforeActivate(AsyncCommand<?, ?, ?> cmd, CompletableFuture<Boolean> initializedFuture,
            ChannelHandlerContext ctx, ClientResources clientResources, Duration timeout) throws Exception {

        ctx.fireUserEventTriggered(new PingBeforeActivate(cmd));

        Runnable timeoutGuard = () -> {

            if (cmd.isDone() || initializedFuture.isDone()) {
                return;
            }

            initializedFuture.completeExceptionally(new RedisCommandTimeoutException(String.format(
                    "Cannot initialize channel (PING before activate) within %s", timeout)));
        };

        Timeout timeoutHandle = clientResources.timer().newTimeout(t -> {

            if (clientResources.eventExecutorGroup().isShuttingDown()) {
                timeoutGuard.run();
                return;
            }

            clientResources.eventExecutorGroup().submit(timeoutGuard);
        }, timeout.toNanos(), TimeUnit.NANOSECONDS);

        cmd.whenComplete((o, throwable) -> {

            timeoutHandle.cancel();

            if (throwable == null) {
                ctx.fireChannelActive();
                initializedFuture.complete(true);
            } else {
                initializedFuture.completeExceptionally(throwable);
            }
        });

    }

    @Override
    public CompletableFuture<Boolean> channelInitialized() {
        return initializedFuture;
    }

}
