/**
 * Copyright (c) 2013-2026 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.misc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Wraps a collection with a proxy that fires callbacks on mutation.
 * Supported mutations: add, addAll, remove, removeAll, clear, set.
 * Unsupported: removeIf, retainAll, replaceAll, sort.
 * subList returns an unmodifiable view.
 *
 * @author ngyngcphu
 */
public final class CollectionSyncProxy {

    private CollectionSyncProxy() {
    }

    public static <E> Collection<E> wrap(
            Collection<E> delegate,
            Consumer<E> onAdd,
            Consumer<E> onRemove,
            BiConsumer<E, E> onReplace,
            Runnable onClear) {

        InvocationHandler handler = (proxy, method, args) -> {
            String name = method.getName();

            switch (name) {
                case "iterator":
                    return wrapIterator(delegate, onRemove);
                case "listIterator":
                    if (delegate instanceof List) {
                        return wrapListIterator((List<E>) delegate, args, onAdd, onRemove, onReplace);
                    }
                    return method.invoke(delegate, args);
                case "subList":
                    return Collections.unmodifiableList(
                            ((List<?>) delegate).subList((Integer) args[0], (Integer) args[1]));
                case "contains": case "containsAll": case "size": case "isEmpty":
                case "get": case "indexOf": case "lastIndexOf": case "toArray":
                case "hashCode": case "equals": case "toString":
                case "stream": case "spliterator": case "parallelStream":
                case "forEach":
                    return method.invoke(delegate, args);

                case "add": {
                    Object result = method.invoke(delegate, args);
                    if (args.length >= 1) {
                        Object element = args[args.length - 1];
                        if (element != null) {
                            onAdd.accept((E) element);
                        }
                    }
                    return result;
                }
                case "addAll": {
                    Object result = method.invoke(delegate, args);
                    Collection<?> toAdd;
                    if (args.length == 2) {
                        toAdd = (Collection<?>) args[1];
                    } else {
                        toAdd = (Collection<?>) args[0];
                    }
                    for (Object e : toAdd) {
                        if (e != null) {
                            onAdd.accept((E) e);
                        }
                    }
                    return result;
                }
                case "remove": {
                    Object result = method.invoke(delegate, args);
                    if (args.length == 1) {
                        if (delegate instanceof List && method.getParameterTypes().length == 1
                                && method.getParameterTypes()[0] == int.class) {
                            Object removed = result;
                            if (removed != null) {
                                onRemove.accept((E) removed);
                            }
                        } else if (Boolean.TRUE.equals(result) && args[0] != null) {
                            onRemove.accept((E) args[0]);
                        }
                    }
                    return result;
                }
                case "removeAll": {
                    Object result = method.invoke(delegate, args);
                    if (args.length == 1 && Boolean.TRUE.equals(result)) {
                        for (Object e : (Collection<?>) args[0]) {
                            if (e != null) {
                                onRemove.accept((E) e);
                            }
                        }
                    }
                    return result;
                }
                case "clear": {
                    Object result = method.invoke(delegate, args);
                    onClear.run();
                    return result;
                }
                case "set": {
                    Object result = method.invoke(delegate, args);
                    if (args.length >= 2) {
                        onReplace.accept((E) result, (E) args[1]);
                    }
                    return result;
                }

                default:
                    throw new UnsupportedOperationException(
                            method.getName() + " is not supported on indexed collections. "
                            + "Use add/remove/iterator() instead.");
            }
        };

        Set<Class<?>> interfaces = new LinkedHashSet<>();
        for (Class<?> c = delegate.getClass(); c != null; c = c.getSuperclass()) {
            interfaces.addAll(Arrays.asList(c.getInterfaces()));
        }
        return (Collection<E>) Proxy.newProxyInstance(
                delegate.getClass().getClassLoader(),
                interfaces.toArray(new Class<?>[0]),
                handler);
    }

    private static <E> Iterator<E> wrapIterator(Collection<E> delegate, Consumer<E> onRemove) {
        Iterator<E> it = delegate.iterator();
        return new Iterator<E>() {
            E lastReturned;
            public boolean hasNext() {
                return it.hasNext();
            }
            public E next() {
                lastReturned = it.next();
                return lastReturned;
            }
            public void remove() {
                it.remove();
                if (lastReturned != null) {
                    onRemove.accept(lastReturned);
                    lastReturned = null;
                }
            }
        };
    }

    private static <E> ListIterator<E> wrapListIterator(
            List<E> delegate, Object[] args, Consumer<E> onAdd, Consumer<E> onRemove, BiConsumer<E, E> onReplace) {
        ListIterator<E> it;
        if (args != null && args.length > 0) {
            it = delegate.listIterator((Integer) args[0]);
        } else {
            it = delegate.listIterator();
        }
        return new ListIterator<E>() {
            E lastReturned;
            public boolean hasNext() {
                return it.hasNext();
            }
            public boolean hasPrevious() {
                return it.hasPrevious();
            }
            public E next() {
                lastReturned = it.next();
                return lastReturned;
            }
            public E previous() {
                lastReturned = it.previous();
                return lastReturned;
            }
            public int nextIndex() {
                return it.nextIndex();
            }
            public int previousIndex() {
                return it.previousIndex();
            }
            public void remove() {
                it.remove();
                if (lastReturned != null) {
                    onRemove.accept(lastReturned);
                    lastReturned = null;
                }
            }
            public void set(E e) {
                it.set(e);
                if (lastReturned != null) {
                    onReplace.accept(lastReturned, e);
                    lastReturned = e;
                }
            }
            public void add(E e) {
                it.add(e);
                if (e != null) {
                    onAdd.accept(e);
                }
                lastReturned = null;
            }
        };
    }
}
