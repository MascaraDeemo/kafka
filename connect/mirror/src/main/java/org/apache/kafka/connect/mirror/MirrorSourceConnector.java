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

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreateTopicsOptions;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replicate data, configuration, and ACLs between clusters.
 *
 *  @see MirrorConnectorConfig for supported config properties.
 */
public class MirrorSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceConnector.class);
    private static final ResourcePatternFilter ANY_TOPIC = new ResourcePatternFilter(ResourceType.TOPIC,
        null, PatternType.ANY);
    private static final AclBindingFilter ANY_TOPIC_ACL = new AclBindingFilter(ANY_TOPIC, AccessControlEntryFilter.ANY);

    private Scheduler scheduler;
    private MirrorConnectorConfig config;
    private SourceAndTarget sourceAndTarget;
    private String connectorName;
    private TopicFilter topicFilter;
    private ConfigPropertyFilter configPropertyFilter;
    private List<TopicPartition> knownTopicPartitions = Collections.emptyList();
    private Set<String> knownTargetTopics = Collections.emptySet();
    private ReplicationPolicy replicationPolicy;
    private int replicationFactor;
    private AdminClient sourceAdminClient;
    private AdminClient targetAdminClient;

    public MirrorSourceConnector() {
        // nop
    }

    // visible for testing
    MirrorSourceConnector(SourceAndTarget sourceAndTarget, ReplicationPolicy replicationPolicy,
            TopicFilter topicFilter, ConfigPropertyFilter configPropertyFilter) {
        this.sourceAndTarget = sourceAndTarget;
        this.replicationPolicy = replicationPolicy;
        this.topicFilter = topicFilter;
        this.configPropertyFilter = configPropertyFilter;
    } 

    @Override
    public void start(Map<String, String> props) {
        long start = System.currentTimeMillis();
        config = new MirrorConnectorConfig(props);
        if (!config.enabled()) {
            return;
        }
        connectorName = config.connectorName();
        sourceAndTarget = new SourceAndTarget(config.sourceClusterAlias(), config.targetClusterAlias());
        topicFilter = config.topicFilter();
        configPropertyFilter = config.configPropertyFilter();
        replicationPolicy = config.replicationPolicy();
        replicationFactor = config.replicationFactor();
        sourceAdminClient = AdminClient.create(config.sourceAdminConfig());
        targetAdminClient = AdminClient.create(config.targetAdminConfig());
        scheduler = new Scheduler(MirrorSourceConnector.class, config.adminTimeout());
        // mm2-offset-syncs.source.internal
        scheduler.execute(this::createOffsetSyncsTopic, "creating upstream offset-syncs topic");
        scheduler.execute(this::loadTopicPartitions, "loading initial set of topic-partitions");
        scheduler.execute(this::createTopicPartitions, "creating downstream topic-partitions");
        scheduler.execute(this::refreshKnownTargetTopics, "refreshing known target topics");
        // acl的同步就先不看了 回头可以通过配置先关掉
        scheduler.scheduleRepeating(this::syncTopicAcls, config.syncTopicAclsInterval(), "syncing topic ACLs");
        // 默认10分钟同步一次
        scheduler.scheduleRepeating(this::syncTopicConfigs, config.syncTopicConfigsInterval(),
            "syncing topic configs");
        // 这个也是默认10分钟一次
        scheduler.scheduleRepeatingDelayed(this::refreshTopicPartitions, config.refreshTopicsInterval(),
            "refreshing topics");
        // 检查有没有这两个日志 打了就说明topic也创建好了，topicConfig啊acl啊什么的都同步了
        log.info("Started {} with {} topic-partitions.", connectorName, knownTopicPartitions.size());
        log.info("Starting {} took {} ms.", connectorName, System.currentTimeMillis() - start);
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        if (!config.enabled()) {
            return;
        }
        Utils.closeQuietly(scheduler, "scheduler");
        Utils.closeQuietly(topicFilter, "topic filter");
        Utils.closeQuietly(configPropertyFilter, "config property filter");
        Utils.closeQuietly(sourceAdminClient, "source admin client");
        Utils.closeQuietly(targetAdminClient, "target admin client");
        log.info("Stopping {} took {} ms.", connectorName, System.currentTimeMillis() - start);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorSourceTask.class;
    }

    // divide topic-partitions among tasks
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (!config.enabled() || knownTopicPartitions.isEmpty()) {
            return Collections.emptyList();
        }
        int numTasks = Math.min(maxTasks, knownTopicPartitions.size());
        return ConnectorUtils.groupPartitions(knownTopicPartitions, numTasks).stream()
            .map(config::taskConfigForTopicPartitions)
            .collect(Collectors.toList());
    }

    @Override
    public ConfigDef config() {
        return MirrorConnectorConfig.CONNECTOR_CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1";
    }

    private List<TopicPartition> findTopicPartitions()
            throws InterruptedException, ExecutionException {
        // 拿到主端的全量topic然后确认是不是在黑白名单、是不是internal topic，然后弄成Set
        Set<String> topics = listTopics(sourceAdminClient).stream()
            .filter(this::shouldReplicateTopic)
            .collect(Collectors.toSet());
        // 这里发describeTopics请求给主端，会返回全量的topic、List<TopicPartitionInfo> partitions、是不是internal和acl相关的东西
        // 第二部flatmap，把TopicDescription变成一个一个的TopicPartition，多个partition的就会有多个TopicPartition在里面
        return describeTopics(topics).stream()
            .flatMap(MirrorSourceConnector::expandTopicDescription)
            .collect(Collectors.toList());
    }

    private void refreshTopicPartitions()
            throws InterruptedException, ExecutionException {
        // 去主端拿全量topic x partition
        List<TopicPartition> topicPartitions = findTopicPartitions();
        Set<TopicPartition> newTopicPartitions = new HashSet<>();
        newTopicPartitions.addAll(topicPartitions);
        // 先全加进来，然后把备端有的给去掉
        newTopicPartitions.removeAll(knownTopicPartitions);
        Set<TopicPartition> deadTopicPartitions = new HashSet<>();
        deadTopicPartitions.addAll(knownTopicPartitions);
        // 先把备端所有的加进来，再把主端的全去掉
        deadTopicPartitions.removeAll(topicPartitions);
        if (!newTopicPartitions.isEmpty() || !deadTopicPartitions.isEmpty()) {
            log.info("Found {} topic-partitions on {}. {} are new. {} were removed. Previously had {}.",
                    topicPartitions.size(), sourceAndTarget.source(), newTopicPartitions.size(), 
                    deadTopicPartitions.size(), knownTopicPartitions.size());
            log.trace("Found new topic-partitions: {}", newTopicPartitions);
            // 更新一下
            knownTopicPartitions = topicPartitions;
            knownTargetTopics = findExistingTargetTopics(); 
            createTopicPartitions();
            context.requestTaskReconfiguration();
        }
    }

    // 去主端拿全量的topic信息 然后在备端来看有没有
    private void loadTopicPartitions()
            throws InterruptedException, ExecutionException {
        knownTopicPartitions = findTopicPartitions();
        knownTargetTopics = findExistingTargetTopics(); 
    }

    private void refreshKnownTargetTopics()
            throws InterruptedException, ExecutionException {
        // 刷新一把备端属于主端的topic
        knownTargetTopics = findExistingTargetTopics();
    }

    private Set<String> findExistingTargetTopics()
            throws InterruptedException, ExecutionException {
        // 在备端列出全量的topic信息 然后找有对应replicationPolicy前缀的
        return listTopics(targetAdminClient).stream()
            .filter(x -> sourceAndTarget.source().equals(replicationPolicy.topicSource(x)))
            .collect(Collectors.toSet());
    }

    private Set<String> topicsBeingReplicated() {
        return knownTopicPartitions.stream()
            .map(x -> x.topic())
            .distinct()
            .filter(x -> knownTargetTopics.contains(formatRemoteTopic(x)))
            .collect(Collectors.toSet());
    }

    private void syncTopicAcls()
            throws InterruptedException, ExecutionException {
        List<AclBinding> bindings = listTopicAclBindings().stream()
            .filter(x -> x.pattern().resourceType() == ResourceType.TOPIC)
            .filter(x -> x.pattern().patternType() == PatternType.LITERAL)
            .filter(this::shouldReplicateAcl)
            .filter(x -> shouldReplicateTopic(x.pattern().name()))
            .map(this::targetAclBinding)
            .collect(Collectors.toList());
        updateTopicAcls(bindings);
    }

    private void syncTopicConfigs()
            throws InterruptedException, ExecutionException {
        // 从之前更新好的备端有的主端topic数据集里面拿出所有topic来describeTopicConfig
        Map<String, Config> sourceConfigs = describeTopicConfigs(topicsBeingReplicated());
        // 这里Map的String是已经搞成了加了前缀的，Config已经筛选成了需要同步的那些
        Map<String, Config> targetConfigs = sourceConfigs.entrySet().stream()
            .collect(Collectors.toMap(x -> formatRemoteTopic(x.getKey()), x -> targetConfig(x.getValue())));
        // 用备端的adminClient去更改topic配置
        updateTopicConfigs(targetConfigs);
    }

    private void createOffsetSyncsTopic() {
        MirrorUtils.createSinglePartitionCompactedTopic(config.offsetSyncsTopic(), config.offsetSyncsTopicReplicationFactor(), config.sourceAdminConfig());
    }

    private void createTopicPartitions()
            throws InterruptedException, ExecutionException {
        // 这一步会把TopicPartition转化成一个Map<TopicName, PartitionNum>的样子
        Map<String, Long> partitionCounts = knownTopicPartitions.stream()
            .collect(Collectors.groupingBy(x -> x.topic(), Collectors.counting())).entrySet().stream()
            .collect(Collectors.toMap(x -> formatRemoteTopic(x.getKey()), x -> x.getValue()));
        // 这里判断在备端有没有，没有的话就创建一个新的NewTopic对象，里面包含了TopicName、topicPartition、和replicationFactor
        List<NewTopic> newTopics = partitionCounts.entrySet().stream()
            .filter(x -> !knownTargetTopics.contains(x.getKey()))
            .map(x -> new NewTopic(x.getKey(), x.getValue().intValue(), (short) replicationFactor))
            .collect(Collectors.toList());
        // 这里会看如果一个topic已经存在了，会去弄成一个Map<TopicName, NewPartitionNum>的样子
        Map<String, NewPartitions> newPartitions = partitionCounts.entrySet().stream()
            .filter(x -> knownTargetTopics.contains(x.getKey()))
            .collect(Collectors.toMap(x -> x.getKey(), x -> NewPartitions.increaseTo(x.getValue().intValue())));
        // 调用备端的adminClient来创建一大堆topic
        targetAdminClient.createTopics(newTopics, new CreateTopicsOptions()).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Could not create topic {}.", k, e);
            } else {
                log.info("Created remote topic {} with {} partitions.", k, partitionCounts.get(k));
            }
        }));
        // 调用备端的adminClient来增加partition
        targetAdminClient.createPartitions(newPartitions).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e instanceof InvalidPartitionsException) {
                // swallow, this is normal
            } else if (e != null) {
                log.warn("Could not create topic-partitions for {}.", k, e);
            } else {
                log.info("Increased size of {} to {} partitions.", k, partitionCounts.get(k));
            }
        }));
    }

    private Set<String> listTopics(AdminClient adminClient)
            throws InterruptedException, ExecutionException {
        return adminClient.listTopics().names().get();
    }

    private Collection<AclBinding> listTopicAclBindings()
            throws InterruptedException, ExecutionException {
        return sourceAdminClient.describeAcls(ANY_TOPIC_ACL).values().get();
    }

    private Collection<TopicDescription> describeTopics(Collection<String> topics)
            throws InterruptedException, ExecutionException {
        return sourceAdminClient.describeTopics(topics).all().get().values();
    }

    @SuppressWarnings("deprecation")
    // use deprecated alterConfigs API for broker compatibility back to 0.11.0
    private void updateTopicConfigs(Map<String, Config> topicConfigs)
            throws InterruptedException, ExecutionException {
        Map<ConfigResource, Config> configs = topicConfigs.entrySet().stream()
            .collect(Collectors.toMap(x ->
                new ConfigResource(ConfigResource.Type.TOPIC, x.getKey()), x -> x.getValue()));
        log.trace("Syncing configs for {} topics.", configs.size());
        targetAdminClient.alterConfigs(configs).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Could not alter configuration of topic {}.", k.name(), e);
            }
        }));
    }

    private void updateTopicAcls(List<AclBinding> bindings)
            throws InterruptedException, ExecutionException {
        log.trace("Syncing {} topic ACL bindings.", bindings.size());
        targetAdminClient.createAcls(bindings).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Could not sync ACL of topic {}.", k.pattern().name(), e);
            }
        }));
    }

    private static Stream<TopicPartition> expandTopicDescription(TopicDescription description) {
        String topic = description.name();
        return description.partitions().stream()
            .map(x -> new TopicPartition(topic, x.partition()));
    }

    private Map<String, Config> describeTopicConfigs(Set<String> topics)
            throws InterruptedException, ExecutionException {
        Set<ConfigResource> resources = topics.stream()
            .map(x -> new ConfigResource(ConfigResource.Type.TOPIC, x))
            .collect(Collectors.toSet());
        return sourceAdminClient.describeConfigs(resources).all().get().entrySet().stream()
            .collect(Collectors.toMap(x -> x.getKey().name(), x -> x.getValue()));
    }

    Config targetConfig(Config sourceConfig) {
        List<ConfigEntry> entries = sourceConfig.entries().stream()
            .filter(x -> !x.isDefault() && !x.isReadOnly() && !x.isSensitive())
            .filter(x -> x.source() != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
            .filter(x -> shouldReplicateTopicConfigurationProperty(x.name()))
            .collect(Collectors.toList());
        return new Config(entries);
    }

    private static AccessControlEntry downgradeAllowAllACL(AccessControlEntry entry) {
        return new AccessControlEntry(entry.principal(), entry.host(), AclOperation.READ, entry.permissionType());
    }

    AclBinding targetAclBinding(AclBinding sourceAclBinding) {
        String targetTopic = formatRemoteTopic(sourceAclBinding.pattern().name());
        final AccessControlEntry entry;
        if (sourceAclBinding.entry().permissionType() == AclPermissionType.ALLOW
                && sourceAclBinding.entry().operation() == AclOperation.ALL) {
            entry = downgradeAllowAllACL(sourceAclBinding.entry());
        } else {
            entry = sourceAclBinding.entry();
        }
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, targetTopic, PatternType.LITERAL), entry);
    }

    boolean shouldReplicateTopic(String topic) {
        return (topicFilter.shouldReplicateTopic(topic) || isHeartbeatTopic(topic))
            && !replicationPolicy.isInternalTopic(topic) && !isCycle(topic);
    }

    boolean shouldReplicateAcl(AclBinding aclBinding) {
        return !(aclBinding.entry().permissionType() == AclPermissionType.ALLOW
            && aclBinding.entry().operation() == AclOperation.WRITE);
    }

    boolean shouldReplicateTopicConfigurationProperty(String property) {
        return configPropertyFilter.shouldReplicateConfigProperty(property);
    }

    // Recurse upstream to detect cycles, i.e. whether this topic is already on the target cluster
    boolean isCycle(String topic) {
        String source = replicationPolicy.topicSource(topic);
        if (source == null) {
            return false;
        } else if (source.equals(sourceAndTarget.target())) {
            return true;
        } else {
            return isCycle(replicationPolicy.upstreamTopic(topic));
        }
    }

    // e.g. heartbeats, us-west.heartbeats
    boolean isHeartbeatTopic(String topic) {
        return MirrorClientConfig.HEARTBEATS_TOPIC.equals(replicationPolicy.originalTopic(topic));
    }

    String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceAndTarget.source(), topic);
    }
}
