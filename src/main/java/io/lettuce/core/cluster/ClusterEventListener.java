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

/**
 *集群事件监听器
 */
interface ClusterEventListener {

    //ask是触发
    void onAskRedirection();
    //moved触发
    void onMovedRedirection();

    void onReconnection(int attempt);

    static ClusterEventListener NO_OP = new ClusterEventListener() {
        @Override
        public void onAskRedirection() {

        }

        @Override
        public void onMovedRedirection() {

        }

        @Override
        public void onReconnection(int attempt) {

        }
    };
}
