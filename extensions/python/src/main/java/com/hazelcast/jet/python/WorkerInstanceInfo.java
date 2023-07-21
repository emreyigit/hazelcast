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


/**
 * Holds information about worker instance which is a container runs user code
 * and communication over grpc service.
 */
class WorkerInstanceInfo {

    private String name;
    private String image;
    private String address;
    private int port;

    WorkerInstanceInfo(String name, String image, String address, int port) {
        this.name = name;
        this.image = image;
        this.address = address;
        this.port = port;
    }

    public String getName() {
        return name;
    }

    public String getImage() {
        return image;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }
}
