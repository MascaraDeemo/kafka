/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.ArgumentParsers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.io.File;

/**
 *  Entry point for "MirrorMaker 2.0".
 *  <p>
 *  MirrorMaker runs a set of Connectors between multiple clusters, in order to replicate data, configuration,
 *  ACL rules, and consumer group state.
 *  </p>
 *  <p>
 *  Configuration is via a top-level "mm2.properties" file, which supports per-cluster and per-replication
 *  sub-configs. Each source->target replication must be explicitly enabled. For example:
 *  </p>
 *  <pre>
 *    clusters = primary, backup
 *    primary.bootstrap.servers = vip1:9092
 *    backup.bootstrap.servers = vip2:9092
 *    primary->backup.enabled = true
 *    backup->primary.enabled = true
 *  </pre>
 *  <p>
 *  Run as follows:
 *  </p>
 *  <pre>
 *    ./bin/connect-mirror-maker.sh mm2.properties
 *  </pre>
 *  <p>
 *  Additional information and example configurations are provided in ./connect/mirror/README.md
 *  </p>
 */
public class MirrorMaker {
    private static final Logger log = LoggerFactory.getLogger(MirrorMaker.class);

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60L;
    private static final ConnectorClientConfigOverridePolicy CLIENT_CONFIG_OVERRIDE_POLICY =
            new AllConnectorClientConfigOverridePolicy();

    private static final List<Class> CONNECTOR_CLASSES = Arrays.asList(
        MirrorSourceConnector.class,
        MirrorHeartbeatConnector.class,
        MirrorCheckpointConnector.class);
 
    private final Map<SourceAndTarget, Herder> herders = new HashMap<>();
    private CountDownLatch startLatch;
    private CountDownLatch stopLatch;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ShutdownHook shutdownHook;
    private final String advertisedBaseUrl;
    private final Time time;
    private final MirrorMakerConfig config;
    private final Set<String> clusters;
    private final Set<SourceAndTarget> herderPairs;

    /**
     * @param config    MM2 configuration from mm2.properties file
     * @param clusters  target clusters for this node. These must match cluster
     *                  aliases as defined in the config. If null or empty list,
     *                  uses all clusters in the config.
     * @param time      time source
     */
    public MirrorMaker(MirrorMakerConfig config, List<String> clusters, Time time) {
        log.debug("Kafka MirrorMaker instance created");
        this.time = time;
        this.advertisedBaseUrl = "NOTUSED";
        this.config = config;
        if (clusters != null && !clusters.isEmpty()) {
            this.clusters = new HashSet<>(clusters);
        } else {
            // default to all clusters
            this.clusters = config.clusters();
        }
        // 可以注意一下这行日志的输出
        log.info("Targeting clusters {}", this.clusters);
        this.herderPairs = config.clusterPairs().stream()
            // 这里可以看出cluster这个东西一定是正确的全部是备端的
            // 这里去filter了只有当x的target在this.cluster中时才保留
            // 所以不存在会自己节点跟自己节点的情况？ 那备端是不是还是会自己节点跟自己节点？
            // 这里也应该吧herderPairs打出来看一下：
            // 已验证 这里理解错了 这个cluster其实是String。
            // 比如cluster = primary, backup
            .filter(x -> this.clusters.contains(x.target()))
            .collect(Collectors.toSet());
        if (herderPairs.isEmpty()) {
            throw new IllegalArgumentException("No source->target replication flows.");
        }
        this.herderPairs.forEach(x -> addHerder(x));
        shutdownHook = new ShutdownHook();
    }

    /**
     * @param config    MM2 configuration from mm2.properties file
     * @param clusters  target clusters for this node. These must match cluster
     *                  aliases as defined in the config. If null or empty list,
     *                  uses all clusters in the config.
     * @param time      time source
     */
    public MirrorMaker(Map<String, String> config, List<String> clusters, Time time) {
        this(new MirrorMakerConfig(config), clusters, time);
    }

    public MirrorMaker(Map<String, String> props, List<String> clusters) {
        this(props, clusters, Time.SYSTEM);
    }

    public MirrorMaker(Map<String, String> props) {
        this(props, null);
    }


    public void start() {
        log.info("Kafka MirrorMaker starting with {} herders.", herders.size());
        if (startLatch != null) {
            throw new IllegalStateException("MirrorMaker instance already started");
        }
        startLatch = new CountDownLatch(herders.size());
        stopLatch = new CountDownLatch(herders.size());
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        for (Herder herder : herders.values()) {
            try {
                herder.start();
            } finally {
                startLatch.countDown();
            }
        }
        log.info("Configuring connectors...");
        herderPairs.forEach(x -> configureConnectors(x));
        log.info("Kafka MirrorMaker started");
    }

    public void stop() {
        boolean wasShuttingDown = shutdown.getAndSet(true);
        if (!wasShuttingDown) {
            log.info("Kafka MirrorMaker stopping");
            for (Herder herder : herders.values()) {
                try {
                    herder.stop();
                } finally {
                    stopLatch.countDown();
                }
            }
            log.info("Kafka MirrorMaker stopped.");
        }
    }

    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for MirrorMaker to shutdown");
        }
    }

    private void configureConnector(SourceAndTarget sourceAndTarget, Class connectorClass) {
        checkHerder(sourceAndTarget);
        Map<String, String> connectorProps = config.connectorBaseConfig(sourceAndTarget, connectorClass);
        herders.get(sourceAndTarget)
                .putConnectorConfig(connectorClass.getSimpleName(), connectorProps, true, (e, x) -> {
                    if (e instanceof NotLeaderException) {
                        // No way to determine if the connector is a leader or not beforehand.
                        log.info("Connector {} is a follower. Using existing configuration.", sourceAndTarget);
                    } else {
                        log.info("Connector {} configured.", sourceAndTarget, e);
                    }
                });
    }

    private void checkHerder(SourceAndTarget sourceAndTarget) {
        if (!herders.containsKey(sourceAndTarget)) {
            throw new IllegalArgumentException("No herder for " + sourceAndTarget.toString());
        }
    }

    private void configureConnectors(SourceAndTarget sourceAndTarget) {
        CONNECTOR_CLASSES.forEach(x -> configureConnector(sourceAndTarget, x));
    }

    private void addHerder(SourceAndTarget sourceAndTarget) {
        log.info("creating herder for " + sourceAndTarget.toString());
        // 内有配置项的详细解析（同样吃connect本身的配置
        Map<String, String> workerProps = config.workerConfig(sourceAndTarget);
        // 目前是在构造方法内定义这个advertisedBaseUrl，永远等于NOTUSED，这里就是NOTUSED/primary
        String advertisedUrl = advertisedBaseUrl + "/" + sourceAndTarget.source();
        // primary->backup
        String workerId = sourceAndTarget.toString();
        // 这里的代码量真的很大，初始化类加载器 然后把connector、converters、headerConverters
        // transformations、restExtensions、connectorClientConfigPolicies全部初始化然后读进来
        // 然后把delegatingLoader全部都加载好
        Plugins plugins = new Plugins(workerProps);
        // 这里比较现在的类加载器是不是delegatingLoader如果不是就换成delegatingLoader
        // delegate是代表，授权的意思
        plugins.compareAndSwapWithDelegatingLoader();
        // 基本就是继承了所有的workerProps然后用算法验证了一遍
        DistributedConfig distributedConfig = new DistributedConfig(workerProps);
        // 创建一个kafkaAdminClient发送describeCluster请求来拿clusterId
        String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(distributedConfig);
        // 初始化KafkaOffsetBackingStore
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(distributedConfig);
        Worker worker = new Worker(workerId, time, plugins, distributedConfig, offsetBackingStore, CLIENT_CONFIG_OVERRIDE_POLICY);
        WorkerConfigTransformer configTransformer = worker.configTransformer();
        Converter internalValueConverter = worker.getInternalValueConverter();
        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, internalValueConverter);
        statusBackingStore.configure(distributedConfig);
        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                internalValueConverter,
                distributedConfig,
                configTransformer);
        Herder herder = new DistributedHerder(distributedConfig, time, worker,
                kafkaClusterId, statusBackingStore, configBackingStore,
                advertisedUrl, CLIENT_CONFIG_OVERRIDE_POLICY);
        herders.put(sourceAndTarget, herder);
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                if (!startLatch.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.error("Timed out in shutdown hook waiting for MirrorMaker startup to finish. Unable to shutdown cleanly.");
                }
            } catch (InterruptedException e) {
                log.error("Interrupted in shutdown hook while waiting for MirrorMaker startup to finish. Unable to shutdown cleanly.");
            } finally {
                MirrorMaker.this.stop();
            }
        }
    }

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("connect-mirror-maker");
        parser.description("MirrorMaker 2.0 driver");
        parser.addArgument("config").type(Arguments.fileType().verifyCanRead())
            .metavar("mm2.properties").required(true)
            .help("MM2 configuration file.");

        // 不是必须要有的参数，对应的是备端的cluster信息
        parser.addArgument("--clusters").nargs("+").metavar("CLUSTER").required(false)
            .help("Target cluster to use for this node.");
        Namespace ns;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
            return;
        }
        File configFile = (File) ns.get("config");
        // 如果没有配置--cluster这个参数的话在这里的cluster是空的
        List<String> clusters = ns.getList("clusters");
        try {
            log.info("Kafka MirrorMaker initializing ...");

            // 从mm2.properties文件读取参数
            Properties props = Utils.loadProps(configFile.getPath());
            // 把配置项转成参数
            Map<String, String> config = Utils.propsToStringMap(props);
            // cluster可以为空传入构造函数，如果是空的则会从配置文件里面取
            MirrorMaker mirrorMaker = new MirrorMaker(config, clusters, Time.SYSTEM);
            
            try {
                mirrorMaker.start();
            } catch (Exception e) {
                log.error("Failed to start MirrorMaker", e);
                mirrorMaker.stop();
                Exit.exit(3);
            }

            mirrorMaker.awaitStop();

        } catch (Throwable t) {
            log.error("Stopping due to error", t);
            Exit.exit(2);
        }
    }

}
