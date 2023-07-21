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

import com.hazelcast.logging.ILogger;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

class WorkerService {

    private final MediaType json = MediaType.parse("application/json; charset=utf-8");
    private final ILogger logger;
    private final String host = "127.0.0.1";
    private final String baseUrl = "/api/v1";
    private final int defaultPort = 8080;
    private final int defaultGrpcPort = 8090;
    private final OkHttpClient client;
    private final int httpTimeoutSeconds = 60;

    WorkerService(PythonWorkerContext context) {
        logger = context.logger();
        client = (new OkHttpClient.Builder())
                .callTimeout(httpTimeoutSeconds, TimeUnit.SECONDS)
                .readTimeout(httpTimeoutSeconds, TimeUnit.SECONDS)
                .writeTimeout(httpTimeoutSeconds, TimeUnit.SECONDS)
                .connectTimeout(httpTimeoutSeconds, TimeUnit.SECONDS).build();
    }

    boolean createWorkerInstance(String name) {
        JSONObject reqBody = new JSONObject();
        reqBody.put("name", name);
        try {
            return sendPostRequest("/userCodeProcess", reqBody.toString()).isSuccessful();
        } catch (Exception e) {
            logger.warning(e.toString());
            return false;
        }
    }

    public boolean createWorker(String name, String image) {
        JSONObject reqBody = new JSONObject();
        reqBody.put("functionCode", new JSONObject().put("image", image));
        reqBody.put("name", name);
        try {
            Response resp = sendPostRequest("/userCode", reqBody.toString());
            logger.info(reqBody.toString());
            if (!resp.isSuccessful()) {
                logger.severe("Couldn't create a worker base for " + name + " - " + resp.body().string());
                return false;
            }
            return true;

        } catch (Exception e) {
            logger.warning(e.toString());
            return false;
        }
    }

    public boolean getWorkerInstance(String name) {
        JSONObject reqBody = new JSONObject();
        reqBody.put("name", name);
        try {
            Response response = sendGetRequest("/userCodeProcess/" + name);

            if (response.isSuccessful() && response.body().toString().contains(name)) {
                return true;
            }

            return false;
        } catch (Exception e) {
            logger.warning(e.toString());
            return false;
        }
    }

    public JSONObject getWorker(String name) {
        JSONObject reqBody = new JSONObject();
        reqBody.put("name", name);
        try {
            Response response = sendGetRequest("/userCode/" + name);

            if (response.isSuccessful()) {
                return new JSONObject(response.body().string());
            }

            return new JSONObject();
        } catch (Exception e) {
            logger.warning(e.toString());
            return new JSONObject();
        }
    }

    public JSONObject getOrCreateWorker(String name, String image) throws Exception {

        JSONObject respWorker = getWorker(name);

        // build image
        if (!respWorker.getString("name").equals(name)) {
            if (!createWorker(name, image)) {
                throw new Exception("Cannot create a worker. It exists or remote worker service having trouble." +
                        " See the logs.");
            }
        }

        // These should be returned from side-car API.
        JSONObject workerObject = new JSONObject();
        workerObject.put("address", host);
        workerObject.put("port", defaultGrpcPort);
        workerObject.put("name", respWorker.get("name"));
        workerObject.put("image", image);

        boolean isInstanceCreated = createWorkerInstance(name);

        if (!isInstanceCreated) {
            throw new Exception("Cannot create the worker instance for " + name + " - " + image);
        }

        return workerObject;
    }

    public boolean stopWorker(String name) {
        try {
            return sendDelRequest("/userCodeProcess/" + name).isSuccessful();
        } catch (Exception e) {
            logger.warning(e.toString());
            return false;
        }
    }

    private Response sendPostRequest(String endpoint, String jsonString) throws IOException, URISyntaxException {
        try {
            URI uri = new URI("http", null, host, defaultPort, baseUrl + endpoint, null, null);
            Request.Builder reqBuilder = new Request.Builder()
                    .url(uri.toURL())
                    .header("Content-Type", "application/json");
            if (jsonString != null) {
                RequestBody body = RequestBody.create(json, jsonString);
                reqBuilder.post(body);
            } else {
                reqBuilder.post(RequestBody.create(null, new byte[0]));
            }
            Request request = reqBuilder.build();
            Call call = client.newCall(request);
            return call.execute();
        } catch (Exception e) {
            logger.warning(e.toString());
            throw e;
        }
    }

    private Response sendDelRequest(String endpoint) throws IOException, URISyntaxException {
        URI uri = new URI("http", null, host, defaultPort, baseUrl + endpoint, null, null);
        try {
            Request.Builder reqBuilder = new Request.Builder()
                    .url(uri.toURL())
                    .header("Content-Type", "application/json");

            reqBuilder.delete();

            Request request = reqBuilder.build();
            Call call = client.newCall(request);
            return call.execute();
        } catch (Exception e) {
            logger.warning(e.toString());
            throw e;
        }
    }

    private Response sendGetRequest(String endpoint) throws IOException, URISyntaxException {
        URI uri = new URI("http", null, host, defaultPort, baseUrl + endpoint, null, null);
        try {
            Request.Builder reqBuilder = new Request.Builder()
                    .url(uri.toURL())
                    .header("Content-Type", "application/json");

            reqBuilder.get();

            Request request = reqBuilder.build();
            Call call = client.newCall(request);
            return call.execute();
        } catch (Exception e) {
            logger.warning(e.toString());
            throw e;
        }
    }

}
