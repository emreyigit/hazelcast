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

package com.hazelcast.jet.python;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import io.grpc.ManagedChannelBuilder;
import org.json.JSONObject;

/*
 * Worker Context
 */
class PythonWorkerContext {
    private WorkerInstanceInfo workerInfo;
    private final WorkerService workerService;
    private final ILogger logger;
    private final BiFunctionEx<String, Integer, ? extends ManagedChannelBuilder<?>> channelFn;

    PythonWorkerContext(ProcessorSupplier.Context context, PythonServiceConfig cfg) {
        logger = context.hazelcastInstance().getLoggingService()
                .getLogger(getClass().getPackage().getName());

        workerService = new WorkerService(this);
        channelFn = cfg.channelFn();

        try {
            JSONObject resp = workerService.getOrCreateWorker(cfg.handlerFunction(), cfg.handlerModule());
            workerInfo = new WorkerInstanceInfo(resp.getString("name"),
                    resp.getString("image"),
                    resp.getString("address"),
                    resp.getInt("port"));

        } catch (Exception e) {
            throw new JetException("PythonService initialization failed: " + e, e);
        }
    }

    public void destroy() {
        workerService.stopWorker(workerInfo.getName());
    }

    public WorkerInstanceInfo getWorkerInfo() {
        return workerInfo;
    }

    public boolean isWorkerInstanceLive() {
        try {
            return workerService.getWorker(workerInfo.getName()).getString("name") != null;
        } catch (Exception e) {
            logger.finest(e);
        }
        return false;
    }

    ILogger logger() {
        return logger;
    }

    public BiFunctionEx<String, Integer, ? extends ManagedChannelBuilder<?>> channelFn() {
        return channelFn;
    }
}
