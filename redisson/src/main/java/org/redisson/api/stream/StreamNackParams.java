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
 *
 * @author lamnt2008
 *
 */
public class StreamNackParams implements StreamNackArgs, StreamMessageIdArgs<StreamNackArgs> {

    private final String groupName;
    private final StreamNackMode mode;
    private StreamMessageId[] ids;
    private Long retryCount;
    private boolean force;

    public StreamNackParams(String groupName, StreamNackMode mode) {
        this.groupName = groupName;
        this.mode = mode;
    }

    @Override
    public StreamNackArgs ids(StreamMessageId... ids) {
        this.ids = ids;
        return this;
    }

    @Override
    public StreamNackArgs retryCount(long count) {
        this.retryCount = count;
        return this;
    }

    @Override
    public StreamNackArgs force() {
        this.force = true;
        return this;
    }

    public String getGroupName() {
        return groupName;
    }

    public StreamNackMode getMode() {
        return mode;
    }

    public StreamMessageId[] getIds() {
        return ids;
    }

    public Long getRetryCount() {
        return retryCount;
    }

    public boolean isForce() {
        return force;
    }
}
