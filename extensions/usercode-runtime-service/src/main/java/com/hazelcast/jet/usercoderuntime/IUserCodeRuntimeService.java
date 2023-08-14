/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.hazelcast.jet.usercoderuntime;

import java.util.concurrent.Future;

/**
 * The service to manage life-cycle of the user code runtime.
 */
public interface IUserCodeRuntimeService {

    /**
     * Creates a user code runtime by using user code runtime controller.
     *
     * @since 5.4
     */
    IUserCodeRuntime startRuntime(String name, RuntimeConfig config);

    /**
     * Kills the runtime container by given id.
     *
     * @since 5.4
     */
    Future destroyRuntimeAsync(String uuid);
}

