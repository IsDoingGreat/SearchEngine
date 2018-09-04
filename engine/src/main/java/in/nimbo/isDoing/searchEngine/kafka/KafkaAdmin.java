package in.nimbo.isDoing.searchEngine.kafka;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaAdmin {
    public static Map<String, Object> getJson() {
        Map<String, Object> data = new HashMap<>();
        String brokers = Engine.getConfigs().get("crawler.urlQueue.kafka.brokers");

        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, "crawler getJson");
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        AdminClient kafka = AdminClient.create(properties);
        DescribeClusterResult describeClusterResult = kafka.describeCluster();

        try {
            data.put("clusterId", describeClusterResult.clusterId().get(1, TimeUnit.SECONDS));
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            data.putIfAbsent("errors", new ArrayList<>());
            ((List)data.get("errors")).add(e.getMessage() == null ? "Unknown Error" : e.getMessage());
        }

        try {
            Collection<Node> nodes = describeClusterResult.nodes().get(2, TimeUnit.SECONDS);
            List<String> servers = new ArrayList<>();
            for (Node node : nodes) {
                servers.add(node.id() + " " + node.host());
            }
            data.put("nodes", servers);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            data.putIfAbsent("errors", new ArrayList<>());
            ((List)data.get("errors")).add(e.getMessage() == null ? "Unknown Error" : e.getMessage());
        }

        try {
            Set<String> topics = kafka.listTopics().names().get();
            Map<String, TopicDescription> all = kafka.describeTopics(topics).all().get();
            List<Map<Object, Object>> topicList = new ArrayList<>();
            for (Map.Entry<String, TopicDescription> topic : all.entrySet()) {
                Map<Object, Object> topicData = new HashMap<>();
                topicData.put("name", topic.getKey());
                topicData.put("isInternal", topic.getValue().isInternal());
                ArrayList<Object> partitionList = new ArrayList<>();
                topicData.put("partitions", partitionList);
                List<TopicPartitionInfo> partitions = topic.getValue().partitions();
                for (TopicPartitionInfo partition : partitions) {
                    Map<String,Object> partitionData = new HashMap<>();
                    partitionData.put("id",partition.partition());
                    partitionData.put("isr",partition.isr().stream().map(Node::id).toArray());
                    partitionData.put("leader",partition.leader().id());
                    partitionData.put("replicas",partition.replicas().stream().map(Node::id).toArray());
                    partitionList.add(partitionData);
                }
                topicList.add(topicData);
            }
            data.put("topics", topicList);
        } catch (InterruptedException | ExecutionException e) {
            data.putIfAbsent("errors", new ArrayList<>());
            ((List)data.get("errors")).add(e.getMessage() == null ? "Unknown Error" : e.getMessage());
        }
        return data;
    }
}
