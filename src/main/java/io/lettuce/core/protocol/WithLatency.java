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

/**
 *  记录延迟的接口，时间单位依赖具体实现
 * @author Mark Paluch
 */
interface WithLatency {

	/**
	 * 设置发送时间
     */
	void sent(long time);

	/**
	 * 设置第一个响应时间
     */
	void firstResponse(long time);

	/**
	 * 设置完成时间
     */
	void completed(long time);

	/**
	 * 获取发送时间
     */
	long getSent();

	/**
	 * 获取第一个响应时间
     */
	long getFirstResponse();

	/**
	 * 获取完成时间
     */
	long getCompleted();

}
