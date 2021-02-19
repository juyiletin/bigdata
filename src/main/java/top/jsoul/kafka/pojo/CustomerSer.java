package top.jsoul.kafka.pojo;

import org.apache.commons.lang.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSer implements Serializer<Customer> {
    public void configure(Map<String, ?> map, boolean b) {
    }

    /**
     * @param topic 分区
     * @param customer 要被序列化的Customer
     * @return 被序列化后的字节数组：ID为4字节整数，Name长度的4字节整数，Name的内容n字节。
     */
    public byte[] serialize(String topic, Customer customer) {
        try {
            byte[] serializedName;
            int stringSize;
            if (customer == null)
                return null;
            else {
                if (customer.getCustomerName() != null) {
                    serializedName = customer.getCustomerName().getBytes("UTF-8");
                    stringSize = customer.getCustomerName().length();
                } else {
                    serializedName = new byte[0];
                    stringSize = 0;
                }
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(customer.getCustomerId());
            buffer.putInt(stringSize);
            buffer.put(serializedName);
            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }
    }

    public void close() {
    }
}
