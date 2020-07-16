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
        // 使用了上面的distributed config，OverridePolicy使用的是AllConnector
        // 如果要修改topic命名格式，是不是需要改这里的Policy？
        Worker worker = new Worker(workerId, time, plugins, distributedConfig, offsetBackingStore, CLIENT_CONFIG_OVERRIDE_POLICY);
        WorkerConfigTransformer configTransformer = worker.configTransformer();
        Converter internalValueConverter = worker.getInternalValueConverter();
        // KafkaStatusBackingStore是一个compacted topic用来存储connector和task的状态
        // 以下几种情况发生时，当前topic中的status可以被认为可以复写
        // 1. 当topic之前没有status信息写入时
        // 2. 上一个写入status信息的worker的worker.id和这次准备要写入的worker是同一个
        // 3. 当前的generation比上一个要高
        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, internalValueConverter);
        // 也是使用了distributedConfig来配置的
        statusBackingStore.configure(distributedConfig);
        // 用来保存Connector和task配置的compacted topic，主要有一下三种
        // 1. Connector配置，是一个Map<String, String>，以connector-[connector-id]为key
        //    这里面的信息不是永久的，如果一个Connect Cluster挂了，这里面的数据很有必要被恢复来保证和原来一致
        // 2. Task配置，也是一个Map<String, String>，以task-[connector-id]-[task-id]为key
        //    这里面的信息也不是永久的，存在这里面有两个目的
        //      （一） 把配置传给所有使用通用配置的worker
        //      （二） 加速挂掉的Connector集群的恢复速度，因为大多数需要恢复的集群的配置都跟挂掉之前是一样的
        // 3. Task的提交情况，是以record的形式进行提交的，以commit-[connector-id]为key
        //    这个信息的存在目的也有两个
        //      （一） 用来记录当前Connector正在执行的Task数量（就可以提高或者降低并行规模
        //      （二） 因为每个Task的配置都是单独存放的，但是所有的Task需要一起被下发来保证每个分区都被分到同一个Task
        //            这个记录同时也代表着对准备下发到的或已经提交的特定Connector的task配置
        // 注意 这个ConfigBackingStore永远都应该只被一个User（worker 写入
        // 在分布式系统中，需要先把config forward到leader节点再进行写入，保证永远只有一个User写
        // 还有一点需要注意的是，Config的更新是在后台执行的，调用者在使用的所有accessors的时候都要注意不要随意的篡改这个值
        // KafkaConfigBackingStore为了保证这一点永远只会暴露一个snapshot给调用者，后台的更新动作不会被影响也不会停
        // 调用者需要确保在使用其中的snapshot时一直用的是同一个，并在安全的时候更新对应的快照
        // 当task config需要worker间的同步保证offset提交和配置的更新时
        // rebalance期间发生的回调和更新必须要推后
        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                internalValueConverter,
                distributedConfig,
                configTransformer);
        // 放牧人？ 用来指挥其他worker，在多进程中下发任务 || 消费者组和消费者的指挥者
        // 每一个加入到这个Herder组里面的worker都享有同样的configuration。
        // group coordinator负责给每一个worker分发工作当前剩余工作的一个子集
        // 当前是使用轮训模式来下发的。放牧人同样可以使用sticky assignment的模式来避免启停woker的消耗
        // 每当收到一些下发任务，放牧人就会把connector和任务放到一个worker中来完成任务
        // 在分布式Herder中，每一个generation中总会有一个leader被选举出来，用来做一些只能一个节点来做的事情
        // 包括写配置项和任务的更新信息（创建，摧毁和扩容/缩容connectors）
        // 大多数的任务分布式Herder都是通过单线程来完成的，包括配置变更、处理重平衡、处理HTTP层的请求
        // 这些任务都会被放到一个队列里面去等待线程有时间来完成。有一个巧合会发生在这里，就是请求可能会因为重平衡而卡住
        // 大多数情况下如果herder知道重平衡要发生了，请求会立刻返回错误，但是总有偶然情况（尤其是其他worker要求的重平衡）
        // 和处理请求一样，配置参数的变更同样也是异步的通过拉配置topic来处理的
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
            // 初始化了一万个东西
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
