/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client;

import com.hazelcast.client.impl.ListenerManager;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;

/**
 * Hazelcast Client enables you to do all Hazelcast operations without
 * being a member of the cluster. It connects to one of the
 * cluster members and delegates all cluster wide operations to it.
 * When the connected cluster member dies, client will
 * automatically switch to another live member.
 */
public class HazelcastClient implements HazelcastInstance {

    private final static AtomicInteger clientIdCounter = new AtomicInteger();

    private final static List<HazelcastClient> lsClients = new CopyOnWriteArrayList<HazelcastClient>();

    final Map<Long, Call> calls = new ConcurrentHashMap<Long, Call>(100);

    final ListenerManager listenerManager;
    final OutRunnable out;
    final InRunnable in;
    final ConnectionManager connectionManager;
    final Map<Object, Object> mapProxies = new ConcurrentHashMap<Object, Object>(100);
    final ConcurrentMap<String, ExecutorServiceClientProxy> mapExecutors = new ConcurrentHashMap<String, ExecutorServiceClientProxy>(2);
    final ClusterClientProxy clusterClientProxy;
    final PartitionClientProxy partitionClientProxy;
    final LifecycleServiceClientImpl lifecycleService;
    final static ILogger logger = Logger.getLogger(HazelcastClient.class.getName());

    final int id;

    private final ClientConfig config;

    private final AtomicBoolean active = new AtomicBoolean(true);

    private HazelcastClient(ClientConfig config) {
        //this.properties = properties;
        if (config.getCredentials() == null) {
            config.setCredentials(new UsernamePasswordCredentials(config.getGroupConfig().getName(),
                    config.getGroupConfig().getPassword()));
        }
        this.config = config;
        this.id = clientIdCounter.incrementAndGet();
        lifecycleService = new LifecycleServiceClientImpl(this);
        lifecycleService.fireLifecycleEvent(STARTING);
        //empty check
        connectionManager = new ConnectionManager(this, config, lifecycleService);
        connectionManager.setBinder(new DefaultClientBinder(this));
        out = new OutRunnable(this, calls, new PacketWriter());
        in = new InRunnable(this, out, calls, new PacketReader());
        listenerManager = new ListenerManager(this);
        try {
            final Connection c = connectionManager.getInitConnection();
            if (c == null) {
                connectionManager.shutdown();
                throw new IllegalStateException("Unable to connect to cluster");
            }
        } catch (IOException e) {
            connectionManager.shutdown();
            throw new ClusterClientException(e.getMessage(), e);
        }
        final String prefix = "hz.client." + this.id + ".";
        new Thread(out, prefix + "OutThread").start();
        new Thread(in, prefix + "InThread").start();
        new Thread(listenerManager, prefix + "Listener").start();
        clusterClientProxy = new ClusterClientProxy(this);
        partitionClientProxy = new PartitionClientProxy(this);
        if (config.isUpdateAutomatic()) {
            this.getCluster().addMembershipListener(connectionManager);
            connectionManager.updateMembers();
        }
        lifecycleService.fireLifecycleEvent(STARTED);
        connectionManager.scheduleHeartbeatTimerTask();
        lsClients.add(HazelcastClient.this);
    }

    GroupConfig groupConfig() {
        return config.getGroupConfig();
    }

    public InRunnable getInRunnable() {
        return in;
    }

    public OutRunnable getOutRunnable() {
        return out;
    }

    ListenerManager getListenerManager() {
        return listenerManager;
    }

    /**
     * @param config
     * @return
     */

    public static HazelcastClient newHazelcastClient(ClientConfig config) {
        return new HazelcastClient(config);
    }
//    /**
//     * Giving address of one member is enough. It will connect to that member and will get addresses of all members
//     * in the cluster. If the connected member will die or leave the cluster, client will automatically
//     * switch to another member in the cluster.
//     *
//     * @param groupName     Group name of a cluster that client will connect
//     * @param groupPassword Group Password of a cluster that client will connect.
//     * @param address       Address of one of the members
//     * @return Returns a new HazelcastClient.
//     */
//    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, String address) {
//        ClientConfig config = new ClientConfig();
//        config.getGroupConfig().setName(groupName);
//        config.getGroupConfig().setPassword(groupPassword);
//        config.addAddress(address);
//        config.setUpdateAutomatic(true);
//        return newHazelcastClient(config);
//    }
//    /**
//     * Giving address of one member is enough. It will connect to that member and will get addresses of all members
//     * in the cluster. If the connected member will die or leave the cluster, client will automatically
//     * switch to another member in the cluster.
//     *
//     * @param clientProperties Client Properties
//     * @param address          Address of one of the members
//     * @return Returns a new HazelcastClient.
//     */
//    public static HazelcastClient newHazelcastClient(ClientProperties clientProperties, String address) {
//        InetSocketAddress inetSocketAddress = parse(address);
//        return new HazelcastClient(clientProperties, inetSocketAddress);
//    }
//    /**
//     * Returns a new HazelcastClient.
//     * <p/>
//     * Giving address of one member is enough. It will connect to that member and will get addresses of all members
//     * in the cluster. If the connected member will die or leave the cluster, client will automatically
//     * switch to another member in the cluster.
//     *
//     * @param credentials {@link Credentials} to be used in authentication
//     * @param address     Address of one of the members that client will choose one to connect.
//     *                    An address is in the form ip:port. If you will not specify the port, it will assume the default one, 5701.
//     *                    ex: "10.90.0.1", "10.90.0.2:5702"
//     * @return Returns a new Hazelcast Client instance.
//     */
//    public static HazelcastClient newHazelcastClient(Credentials credentials, String address) {
//        return newHazelcastClient(credentials, new ClientProperties(), address);
//    }
//    /**
//     * Returns a new HazelcastClient.
//     * <p/>
//     * Giving address of one member is enough. It will connect to that member and will get addresses of all members
//     * in the cluster. If the connected member will die or leave the cluster, client will automatically
//     * switch to another member in the cluster.
//     *
//     * @param credentials      {@link Credentials} to be used in authentication
//     * @param clientProperties Client Properties
//     * @param address          Address of one of the members that client will choose one to connect.
//     *                         An address is in the form ip:port. If you will not specify the port, it will assume the default one, 5701.
//     *                         ex: "10.90.0.1", "10.90.0.2:5702"
//     * @return Returns a new Hazelcast Client instance.
//     */
//    public static HazelcastClient newHazelcastClient(Credentials credentials, ClientProperties clientProperties, String address) {
//        final InetSocketAddress inetSocketAddress = parse(address);
//        ClientConfig config = new ClientConfig();
//        config.setCredentials(credentials);
//        config.getTcpIpConfig().addMember(address);
//        return new HazelcastClient(config, credentials, false, new InetSocketAddress[]{inetSocketAddress}, true);
//    }

    public Config getConfig() {
        throw new UnsupportedOperationException();
    }

    public PartitionService getPartitionService() {
        return partitionClientProxy;
    }

    public LoggingService getLoggingService() {
        throw new UnsupportedOperationException();
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return (IMap<K, V>) getClientProxy(Prefix.MAP + name);
    }

    public <K, V, E> Object getClientProxy(Object o) {
        Object proxy = mapProxies.get(o);
        if (proxy == null) {
            synchronized (mapProxies) {
                proxy = mapProxies.get(o);
                if (proxy == null) {
                    if (o instanceof String) {
                        String name = (String) o;
                        if (name.startsWith(Prefix.MAP)) {
                            proxy = new MapClientProxy<K, V>(this, name);
                        } else if (name.startsWith(Prefix.AS_LIST)) {
                            proxy = new ListClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.SET)) {
                            proxy = new SetClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.QUEUE)) {
                            proxy = new QueueClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.TOPIC)) {
                            proxy = new TopicClientProxy<E>(this, name);
                        } else if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
                            proxy = new AtomicNumberClientProxy(this, name);
                        } else if (name.startsWith(Prefix.COUNT_DOWN_LATCH)) {
                            proxy = new CountDownLatchClientProxy(this, name);
                        } else if (name.startsWith(Prefix.IDGEN)) {
                            proxy = new IdGeneratorClientProxy(this, name);
                        } else if (name.startsWith(Prefix.MULTIMAP)) {
                            proxy = new MultiMapClientProxy(this, name);
                        } else if (name.startsWith(Prefix.SEMAPHORE)) {
                            proxy = new SemaphoreClientProxy(this, name);
                        } else {
                            proxy = new LockClientProxy(o, this);
                        }
                    } else {
                        proxy = new LockClientProxy(o, this);
                    }
                    mapProxies.put(o, proxy);
                }
            }
        }
        return mapProxies.get(o);
    }

    public com.hazelcast.core.Transaction getTransaction() {
        ClientThreadContext trc = ClientThreadContext.get();
        TransactionClientProxy proxy = (TransactionClientProxy) trc.getTransaction(this);
        return proxy;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void addInstanceListener(InstanceListener instanceListener) {
        clusterClientProxy.addInstanceListener(instanceListener);
    }

    public Cluster getCluster() {
        return clusterClientProxy;
    }

    public ExecutorService getExecutorService() {
        return getExecutorService("default");
    }

    public ExecutorService getExecutorService(String name) {
        if (name == null) throw new IllegalArgumentException("ExecutorService name cannot be null");
//        name = Prefix.EXECUTOR_SERVICE + name;
        ExecutorServiceClientProxy executorServiceProxy = mapExecutors.get(name);
        if (executorServiceProxy == null) {
            executorServiceProxy = new ExecutorServiceClientProxy(this, name);
            ExecutorServiceClientProxy old = mapExecutors.putIfAbsent(name, executorServiceProxy);
            if (old != null) {
                executorServiceProxy = old;
            }
        }
        return executorServiceProxy;
    }

    public IdGenerator getIdGenerator(String name) {
        return (IdGenerator) getClientProxy(Prefix.IDGEN + name);
    }

    public AtomicNumber getAtomicNumber(String name) {
        return (AtomicNumber) getClientProxy(Prefix.ATOMIC_NUMBER + name);
    }

    public ICountDownLatch getCountDownLatch(String name) {
        return (ICountDownLatch) getClientProxy(Prefix.COUNT_DOWN_LATCH + name);
    }

    public ISemaphore getSemaphore(String name) {
        return (ISemaphore) getClientProxy(Prefix.SEMAPHORE + name);
    }

    public Collection<Instance> getInstances() {
        return clusterClientProxy.getInstances();
    }

    public <E> IList<E> getList(String name) {
        return (IList<E>) getClientProxy(Prefix.AS_LIST + name);
    }

    public ILock getLock(Object obj) {
        return new LockClientProxy(obj, this);
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return (MultiMap<K, V>) getClientProxy(Prefix.MULTIMAP + name);
    }

    public String getName() {
        return config.getGroupConfig().getName();
    }

    public <E> IQueue<E> getQueue(String name) {
        return (IQueue<E>) getClientProxy(Prefix.QUEUE + name);
    }

    public <E> ISet<E> getSet(String name) {
        return (ISet<E>) getClientProxy(Prefix.SET + name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return (ITopic) getClientProxy(Prefix.TOPIC + name);
    }

    public void removeInstanceListener(InstanceListener instanceListener) {
        clusterClientProxy.removeInstanceListener(instanceListener);
    }

    public static void shutdownAll() {
        for (HazelcastClient hazelcastClient : lsClients) {
            try {
                hazelcastClient.shutdown();
            } catch (Exception ignored) {
            }
        }
        lsClients.clear();
    }

    public void shutdown() {
        lifecycleService.shutdown();
    }

    void doShutdown() {
        if (active.compareAndSet(true, false)) {
            logger.log(Level.INFO, "HazelcastClient[" + this.id + "] is shutting down.");
            connectionManager.shutdown();
            out.shutdown();
            in.shutdown();
            listenerManager.shutdown();
            ClientThreadContext.shutdown();
            lsClients.remove(HazelcastClient.this);
        }
    }

    public boolean isActive() {
        return active.get();
    }

    protected void destroy(String proxyName) {
        mapProxies.remove(proxyName);
    }

    public void restart() {
        lifecycleService.restart();
    }

    public LifecycleService getLifecycleService() {
        return lifecycleService;
    }

    static void runAsyncAndWait(final Runnable runnable) {
        callAsyncAndWait(new Callable<Boolean>() {
            public Boolean call() throws Exception {
                runnable.run();
                return true;
            }
        });
    }

    static <V> V callAsyncAndWait(final Callable<V> callable) {
        final ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            Future<V> future = es.submit(callable);
            try {
                return future.get();
            } catch (Throwable e) {
                logger.log(Level.WARNING, e.getMessage(), e);
                return null;
            }
        } finally {
            es.shutdown();
        }
    }

    public ClientConfig getClientConfig() {
        return config;
    }
}
