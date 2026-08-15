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
package org.redisson.api.stream;

/**
 * Arguments object for RStream.nack() method.
 *
 * @author lamnt2008
 *
 */
public interface StreamNackArgs {

    /**
     * Defines retry count to set for negatively acknowledged messages.
     *
     * @param count retry count
     * @return arguments object
     */
    StreamNackArgs retryCount(long count);

    /**
     * Defines whether to create a pending entry for an existing stream message
     * that is not already pending.
     *
     * @return arguments object
     */
    StreamNackArgs force();

    /**
     * Defines group name and negative acknowledgement mode.
     *
     * @param groupName name of group
     * @param mode negative acknowledgement mode
     * @return next options
     */
    static StreamMessageIdArgs<StreamNackArgs> group(String groupName, StreamNackMode mode) {
        return new StreamNackParams(groupName, mode);
    }
}
