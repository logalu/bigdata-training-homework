package org.geekbang.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.xml.namespace.NamespaceContext;
import java.io.IOException;

public class BaseTest {

    public static void main(String[] args) throws IOException {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2.cluster-285604,emr-worker-1.cluster-285604,emr-header-1.cluster-285604");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.set("hbase.master", "127.0.0.1:16000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("luchangzheng:student");
        createNamespaceIfNotExists(tableName, admin);

        int rowKey = 1;

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor nameFamily = new HColumnDescriptor("name");
            hTableDescriptor.addFamily(nameFamily);
            HColumnDescriptor infoFamily = new HColumnDescriptor("info");
            hTableDescriptor.addFamily(infoFamily);
            HColumnDescriptor scoreFamily = new HColumnDescriptor("score");
            hTableDescriptor.addFamily(scoreFamily);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        String[][] rows = new String[][] {{"Tom", "20210000000001", "1", "75", "82"},
                {"Jerry", "20210000000002", "1", "85", "67"},
                {"Jack", "20210000000003", "2", "80", "80"},
                {"Rose", "20210000000004", "2", "60", "60"}
            };

        for (String[] strs : rows) {
            // 插入数据
            Put put = new Put(Bytes.toBytes(rowKey)); // row key
            put.addColumn(Bytes.toBytes("name"),Bytes.toBytes(""), Bytes.toBytes(strs[0]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes(strs[1])); // col1
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes(strs[2])); // col2
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes(strs[3])); // col1
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes(strs[4])); // col2
            conn.getTable(tableName).put(put);
            rowKey++;
        }


        System.out.println("Data insert success");

        // 查看数据
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        /*// 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }*/
    }

    private static void createNamespaceIfNotExists(TableName tableName, Admin admin) throws IOException {
        String namespaceName = tableName.getNamespaceAsString();
        NamespaceDescriptor ns = NamespaceDescriptor.create(namespaceName).build();
        NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
        boolean exists = false;
        for (NamespaceDescriptor nsd : list) {
            if (nsd.getName().equals(ns.getName())) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            admin.createNamespace(ns);
        }
    }

}
