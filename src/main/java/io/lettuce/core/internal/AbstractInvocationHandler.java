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
package io.lettuce.core.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.*;

/**
 * 抽象对调用处理器
 * @since 4.2
 */
public abstract class AbstractInvocationHandler implements InvocationHandler {

    private static final Object[] NO_ARGS = {};

    /**
     * <p>
     * <ul>
     * <li>{@code proxy.hashCode()} delegates to {@link AbstractInvocationHandler#hashCode}
     * <li>{@code proxy.toString()} delegates to {@link AbstractInvocationHandler#toString}
     * <li>{@code proxy.equals(argument)} returns true if:
     * <ul>
     * <li>{@code proxy} and {@code argument} are of the same type
     * <li>and {@link AbstractInvocationHandler#equals} returns true for the {@link InvocationHandler} of {@code argument}
     * </ul>
     * <li>other method calls are dispatched to {@link #handleInvocation}.
     * </ul>
     *
     * @param proxy the proxy instance that the method was invoked on
     *
     * @param method the {@code Method} instance corresponding to the interface method invoked on the proxy instance. The
     *        declaring class of the {@code Method} object will be the interface that the method was declared in, which may be a
     *        superinterface of the proxy interface that the proxy class inherits the method through.
     *
     * @param args an array of objects containing the values of the arguments passed in the method invocation on the proxy
     *        instance, or {@code null} if interface method takes no arguments. Arguments of primitive types are wrapped in
     *        instances of the appropriate primitive wrapper class, such as {@code java.lang.Integer} or
     *        {@code java.lang.Boolean}.
     * @return the invocation result value
     * @throws Throwable the exception to throw from the method invocation on the proxy instance.
     */
    @Override
    public final Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (args == null) {
            args = NO_ARGS;
        }
        if (args.length == 0 && method.getName().equals("hashCode")) {
            return hashCode();
        }
        if (args.length == 1 && method.getName().equals("equals") && method.getParameterTypes()[0] == Object.class) {
            Object arg = args[0];
            if (arg == null) {
                return false;
            }
            if (proxy == arg) {
                return true;
            }
            return isProxyOfSameInterfaces(arg, proxy.getClass()) && equals(Proxy.getInvocationHandler(arg));
        }
        if (args.length == 0 && method.getName().equals("toString")) {
            return toString();
        }
        return handleInvocation(proxy, method, args);
    }

    /**
     * {@link #invoke} delegates to this method upon any method invocation on the proxy instance, except {@link Object#equals},
     * {@link Object#hashCode} and {@link Object#toString}. The result will be returned as the proxied method's return value.
     *
     * <p>
     * Unlike {@link #invoke}, {@code args} will never be null. When the method has no parameter, an empty array is passed in.
     *
     * @param proxy the proxy instance that the method was invoked on
     *
     * @param method the {@code Method} instance corresponding to the interface method invoked on the proxy instance. The
     *        declaring class of the {@code Method} object will be the interface that the method was declared in, which may be a
     *        superinterface of the proxy interface that the proxy class inherits the method through.
     *
     * @param args an array of objects containing the values of the arguments passed in the method invocation on the proxy
     *        instance, or {@code null} if interface method takes no arguments. Arguments of primitive types are wrapped in
     *        instances of the appropriate primitive wrapper class, such as {@code java.lang.Integer} or
     *        {@code java.lang.Boolean}.
     * @return the invocation result value
     * @throws Throwable the exception to throw from the method invocation on the proxy instance.
     */
    protected abstract Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable;

    /**
     * By default delegates to {@link Object#equals} so instances are only equal if they are identical.
     * {@code proxy.equals(argument)} returns true if:
     * <ul>
     * <li>{@code proxy} and {@code argument} are of the same type
     * <li>and this method returns true for the {@link InvocationHandler} of {@code argument}
     * </ul>
     * <p>
     * Subclasses can override this method to provide custom equality.
     *
     * @param obj the reference object with which to compare.
     * @return {@code true} if this object is the same as the obj argument; {@code false} otherwise.
     */
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    /**
     * By default delegates to {@link Object#hashCode}. The dynamic proxies' {@code hashCode()} will delegate to this method.
     * Subclasses can override this method to provide custom equality.
     *
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * By default delegates to {@link Object#toString}. The dynamic proxies' {@code toString()} will delegate to this method.
     * Subclasses can override this method to provide custom string representation for the proxies.
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return super.toString();
    }

    private static boolean isProxyOfSameInterfaces(Object arg, Class<?> proxyClass) {
        return proxyClass.isInstance(arg)
        // Equal proxy instances should mostly be instance of proxyClass
        // Under some edge cases (such as the proxy of JDK types serialized and then deserialized)
        // the proxy type may not be the same.
        // We first check isProxyClass() so that the common case of comparing with non-proxy objects
        // is efficient.
                || (Proxy.isProxyClass(arg.getClass()) && Arrays.equals(arg.getClass().getInterfaces(),
                        proxyClass.getInterfaces()));
    }

    /**
     * 方法翻译器
     */
    protected static class MethodTranslator {

        private final static WeakHashMap<Class<?>, MethodTranslator> TRANSLATOR_MAP = new WeakHashMap<>(32);

        //真实方法和代理类中方法映射表
        private final Map<Method, Method> map;

        private MethodTranslator(Class<?> delegate, Class<?>... methodSources) {

            map = createMethodMap(delegate, methodSources);
        }

        /**
         * 通过指定代理类，和目标类创建方法翻译器
         */
        public static MethodTranslator of(Class<?> delegate, Class<?>... methodSources) {
            //同步代码块
            synchronized (TRANSLATOR_MAP) {
                //如果翻译器映射表中不存在delegate的翻译器则创建一个新的
                return TRANSLATOR_MAP.computeIfAbsent(delegate, key -> new MethodTranslator(key, methodSources));
            }
        }

        private Map<Method, Method> createMethodMap(Class<?> delegate, Class<?>[] methodSources) {

            Map<Method, Method> map;
            List<Method> methods = new ArrayList<>();
            //遍历源类，找到所有public方法
            for (Class<?> sourceClass : methodSources) {
                methods.addAll(getMethods(sourceClass));
            }

            map = new HashMap<>(methods.size(), 1.0f);

            //创建方法和代理类的方法的映射表
            for (Method method : methods) {

                try {
                    map.put(method, delegate.getMethod(method.getName(), method.getParameterTypes()));
                } catch (NoSuchMethodException ignore) {
                }
            }
            return map;
        }
       //获取目标方法中的所有方法
        private Collection<? extends Method> getMethods(Class<?> sourceClass) {

            //目标方法集合
            Set<Method> result = new HashSet<>();

            Class<?> searchType = sourceClass;
            while (searchType != null && searchType != Object.class) {
                 //将目标类中所有public方法添加到集合中
                result.addAll(filterPublicMethods(Arrays.asList(sourceClass.getDeclaredMethods())));
                //如果souceClass是接口类型
                if (sourceClass.isInterface()) {
                    //获取souceClass的所有接口
                    Class<?>[] interfaces = sourceClass.getInterfaces();
                    //遍历接口，将接口的public方法也添加到方法集合中
                    for (Class<?> interfaceClass : interfaces) {
                        result.addAll(getMethods(interfaceClass));
                    }

                    searchType = null;
                } else {//如果不是接口则查找父类

                    searchType = searchType.getSuperclass();
                }
            }

            return result;
        }

        //获取给定方法集合中所有public方法
        private Collection<? extends Method> filterPublicMethods(List<Method> methods) {
            List<Method> result = new ArrayList<>(methods.size());

            for (Method method : methods) {
                if (Modifier.isPublic(method.getModifiers())) {
                    result.add(method);
                }
            }

            return result;
        }

        public Method get(Method key) {
           //从方法映射表中获取目标方法
            Method result = map.get(key);
            //如果目标方法不为null则返回,否则抛出异常
            if (result != null) {
                return result;
            }
            throw new IllegalStateException("Cannot find source method " + key);
        }
    }
}
