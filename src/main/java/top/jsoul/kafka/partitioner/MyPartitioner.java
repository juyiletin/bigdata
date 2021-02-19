package top.jsoul.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.apache.parquet.io.InvalidRecordException;

import java.util.List;
import java.util.Map;

public class MyPartitioner implements Partitioner {

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if ((keyBytes == null)||(!(key instanceof String)))
            throw new InvalidRecordException("We expect all messages to have customer name as key");

        //Banana被分到最后一个分区，其他随机
        if (((String)key).equals("Banana")){
            return numPartitions;
        } else {
            return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions -1);
        }
    }

    public void close() {
    }

    public void configure(Map<String, ?> map) {
    }
}
