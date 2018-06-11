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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import io.lettuce.core.internal.LettuceAssert;

/**
 * 支持共享锁和独占锁的共享锁门面
 * Shared locking facade that supports shared and exclusive locking.
 * <p>
 * Multiple shared locks (writers) are allowed concurrently to process their work. If an exclusive lock is requested, the
 * exclusive lock requestor will wait until all shared locks are released and the exclusive worker is permitted.
 * <p>
 * Exclusive locking is reentrant. An exclusive lock owner is permitted to acquire and release shared locks. Shared/exclusive
 * lock requests by other threads than the thread which holds the exclusive lock, are forced to wait until the exclusive lock is
 * released.
 *
 * @author Mark Paluch
 */
class SharedLock {
    //写入器计数器
    private final AtomicLong writers = new AtomicLong();
    //持有独占锁的线程
    private volatile Thread exclusiveLockOwner;

    /**
     * Wait for stateLock and increment writers. Will wait if stateLock is locked and if writer counter is negative.
     */
    void incrementWriters() {
        //如果当前线程就是持有独占锁的线程是锁重入，则不需要进行writer计数自增
        if (exclusiveLockOwner == Thread.currentThread()) {
            return;
        }
        //自旋完成计数自增
        synchronized (this) {
            for (;;) {

                if (writers.get() >= 0) {
                    writers.incrementAndGet();
                    return;
                }
            }
        }
    }

    /**
     * Decrement writers without any wait.
     */
    void decrementWriters() {

        if (exclusiveLockOwner == Thread.currentThread()) {
            return;
        }

        writers.decrementAndGet();
    }

    /**
     *使用独占锁模式执行命令
     */
    void doExclusive(Runnable runnable) {

        LettuceAssert.notNull(runnable, "Runnable must not be null");

        doExclusive(() -> {
            runnable.run();
            return null;
        });
    }

    /**
     * Retrieve a value produced by a {@link Supplier} guarded by an exclusive lock.
     *
     * @param supplier the {@link Supplier}, must not be {@literal null}.
     * @param <T> the return type
     * @return the return value
     */
    <T> T doExclusive(Supplier<T> supplier) {

        LettuceAssert.notNull(supplier, "Supplier must not be null");

        synchronized (this) {

            try {

                lockWritersExclusive();
                return supplier.get();
            } finally {
                unlockWritersExclusive();
            }
        }
    }

    /**
     *
     * Wait for stateLock and no writers. Must be used in an outer {@code synchronized} block to prevent interleaving with other
     * methods using writers. Sets writers to a negative value to create a lock for {@link #incrementWriters()}.
     */
    private void lockWritersExclusive() {

        //如果当前线程就是持有独占锁的线程
        if (exclusiveLockOwner == Thread.currentThread()) {
            writers.decrementAndGet();
            return;
        }
       //如果当前线程不是独占锁的线程，则需要等待所有共享锁都释放
        synchronized (this) {
            for (;;) {

                if (writers.compareAndSet(0, -1)) {
                    exclusiveLockOwner = Thread.currentThread();
                    return;
                }
            }
        }
    }

    /**
     * 解锁独占锁
     */
    private void unlockWritersExclusive() {

        if (exclusiveLockOwner == Thread.currentThread()) {
            if (writers.incrementAndGet() == 0) {
                exclusiveLockOwner = null;
            }
        }
    }
}
