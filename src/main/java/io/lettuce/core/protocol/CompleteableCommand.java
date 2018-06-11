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

import java.util.function.Consumer;

/**
 * Extension to commands that provide registration of command completion callbacks. Completion callbacks allow execution of
 * tasks after successive, failed or any completion outcome. A callback must be non-blocking. Callback registration gives no
 * guarantee over callback ordering.
 *
 * @author Mark Paluch
 */
public interface CompleteableCommand<T> {

    /**
     * 注册命令处理结束的回调，使用命令结果或错误信息通知回调方法
     * @param action must not be {@literal null}.
     */
    void onComplete(Consumer<? super T> action);

}
