package top.jsoul.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class Demo {

    public static void main(String[] args) {

//        1. 创建连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("node01", 11111), "example", "", "");

//        指定每次拉取数据的数量
        int batchSize = 1000;
//        定义一个标记不停的拉取数据
        boolean isRunning = true;

//        2. 建立连接
        canalConnector.connect();
//        设置回滚上一次get请求，重新获取数据
        canalConnector.rollback();

//        3. 订阅数据库
        canalConnector.subscribe("test.*");

        while (isRunning) {

//            4. 获取数据
            Message message = canalConnector.getWithoutAck(batchSize);
//            获取batch的id（批次）
            long id = message.getId();
//            getEntries获取封装这批数据的集合
            int size = message.getEntries().size();

            if (size == 0) {
//                没有获取到数据则不处理
            } else {
//                处理数据
                process(message);
            }

//            5. 提交确认
            canalConnector.ack(id);
        }

//        6. 关闭连接
        canalConnector.disconnect();
    }

    private static void process(Message message) {
        //遍历
        for (CanalEntry.Entry entry : message.getEntries()) {
//            如果不是事务的数据，则过滤掉
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }

            CanalEntry.Header header = entry.getHeader();
            String logfileName = header.getLogfileName();
            long logfileOffset = header.getLogfileOffset();
            long executeTime = header.getExecuteTime();
            String schemaName = header.getSchemaName();
            String tableName = header.getTableName();
            String eventTypeName = header.getEventType().toString().toLowerCase();

            System.out.println("logfileName\t" + logfileName);
            System.out.println("logfileOffset\t" + logfileOffset);
            System.out.println("executeTime\t" + executeTime);
            System.out.println("schemaName\t" + schemaName);
            System.out.println("tableName\t" + tableName);
            System.out.println("eventTypeName\t" + eventTypeName);

            CanalEntry.RowChange rowChange = null;
            try {
//                将二进制字节数据解析为RowChange实体
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }

//            迭代每一条变更数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {

//                判断删除
                if (entry.getHeader().getEventType() == CanalEntry.EventType.DELETE) {
                    System.out.println("---delete---");
                    printColumnsList(rowData.getBeforeColumnsList());
                    System.out.println("sql:" + rowChange.getSql());
                    System.out.println("---");
                }
//                判断更新
                else if (entry.getHeader().getEventType() == CanalEntry.EventType.UPDATE) {
                    System.out.println("---update---");
                    printColumnsList(rowData.getBeforeColumnsList());
                    System.out.println("sql:" + rowChange.getSql());
                    printColumnsList(rowData.getAfterColumnsList());
                    System.out.println("---");
                }
//                判断插入
                else if (entry.getHeader().getEventType() == CanalEntry.EventType.INSERT) {
                    System.out.println("---insert---");
                    System.out.println("sql:" + rowChange.getSql());
                    printColumnsList(rowData.getAfterColumnsList());
                    System.out.println("---");
                }
            }


        }
    }

    //    打印列名和列值
    private static void printColumnsList(List<CanalEntry.Column> columnsList) {
//        遍历所有列
        for (CanalEntry.Column column : columnsList) {
            System.out.println(column.getName() + "\t" + column.getValue());
        }
    }
}
