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
package org.redisson.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Reactor {@code Mono}-returning method whose result, when the
 * underlying value is an empty {@link java.util.Map} or
 * {@link java.util.Collection}, should complete without emitting an
 * {@code onNext} signal.
 *
 * <p>Without this annotation, a method returning {@code Mono<Map<K, V>>}
 * whose underlying operation yields an empty map will emit that empty map
 * via {@code onNext} and then complete. With this annotation, the same
 * operation completes the {@code Mono} empty &mdash; allowing reactive
 * pipelines to use {@code switchIfEmpty}, {@code defaultIfEmpty},
 * {@code flatMap}, and similar operators as intended for one-or-zero
 * publishers.
 *
 * @author Nikita Koksharov
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface EmptyAsAbsent {
}