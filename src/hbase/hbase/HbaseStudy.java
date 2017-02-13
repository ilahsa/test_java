//package hbase;
//
//
//
//
//import java.io.IOException;
// 
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.MasterNotRunningException;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.ZooKeeperConnectionException;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
// 
//public class HbaseStudy {
// 
//    public final static Logger logger = LoggerFactory.getLogger(HbaseStudy.class);
// 
//    /* 构建Configuration，这里就是hbase-site.xml解析出来的对象，这里我还指定了本地读取文件的方式 */
//    static Configuration hbaseConf = HBaseConfiguration.create();
//    static {
//        hbaseConf.addResource("hbase-site.xml");
//    }
// 
//    /**
//     * 插入数据
//     * @throws IOException
//     */
//    public void putTableData() throws IOException {
//        HTable tbl = new HTable(hbaseConf, "xsharptable001");
//        Put put = new Put(Bytes.toBytes("xrow01"));
//        put.add(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol01"), Bytes.toBytes("xvalue01"));
//        put.addColumn(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol02"), Bytes.toBytes("xvalue02"));
//        put.addImmutable(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol03"), Bytes.toBytes("xvalue03"));
// 
//        tbl.put(put);
//    }
// 
//    /**
//     * 插入多行数据
//     * @throws IOException
//     */
//    public void putTableDataRow() throws IOException {
//        HTable tbl = new HTable(hbaseConf, "xsharptable001");
//        Put put = new Put(Bytes.toBytes("xrow02"));
//        put.add(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol01"), Bytes.toBytes("xvalue012"));
//        put.addColumn(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol02"), Bytes.toBytes("xvalue022"));
//        put.addImmutable(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol02"), Bytes.toBytes("xvalue032"));
// 
//        tbl.put(put);
// 
//        put = new Put(Bytes.toBytes("xrow03"));
//        put.add(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol01"), Bytes.toBytes("xvalue0213"));
//        put.addColumn(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol02"), Bytes.toBytes("xvalue0123"));
//        put.addImmutable(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol03"), Bytes.toBytes("xvalue0223"));
// 
//        tbl.put(put);
// 
//        put = new Put(Bytes.toBytes("xrow04"));
//        put.add(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol01"), Bytes.toBytes("xvalue0334"));
//        put.addColumn(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol02"), Bytes.toBytes("xvalue0224"));
//        put.addImmutable(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol03"), Bytes.toBytes("xvalue0334"));
//        put.addImmutable(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol04"), Bytes.toBytes("xvalue0334"));
//        tbl.put(put);
//    }
// 
//    /**
//     * 查询hbase表里的数据
//     * @throws IOException
//     */
//    public void getTableData() throws IOException {
//        HTable table = new HTable(hbaseConf, "xsharptable001");
//        Get get = new Get(Bytes.toBytes("xrow01"));
//        get.addFamily(Bytes.toBytes("xcolfam01"));
//        Result result = table.get(get);
//        byte[] bs = result.getValue(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol02"));
//        // ============查询结果:xvalue02
//        logger.info("============查询结果:" + Bytes.toString(bs));
// 
//    }
// 
//    /**
//     * 创建hbase的表
//     *
//     * @throws MasterNotRunningException
//     * @throws ZooKeeperConnectionException
//     * @throws IOException
//     */
//    public void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
//        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
//        if (admin.tableExists(Bytes.toBytes("xsharptable001"))) {
//            logger.info("===============:表已经存在!failure!");
//        } else {
//            TableName tableName = TableName.valueOf(Bytes.toBytes("xsharptable001"));
//            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
//            HColumnDescriptor hcol = new HColumnDescriptor(Bytes.toBytes("xcolfam01"));
//            tableDesc.addFamily(hcol);
//            admin.createTable(tableDesc);
//            logger.info("==============:表创建成功了！Success!");
//        }
//    }
// 
//    /**
//     * 通过scan扫描数据，相当于关系数据的游标
//     *
//     * @throws IOException
//     */
//    public void scanTableData() throws IOException {
//        HTable tbl = new HTable(hbaseConf, "xsharptable001");
//        Scan scanAll = new Scan();
//        ResultScanner scannerAll = tbl.getScanner(scanAll);
//        for (Result resAll : scannerAll) {
//            /*
//             * 打印出来的结果: 2016-06-14 15:46:10,723 INFO [main]
//             * hbasetest.HBaseStudy: ======ScanAll
//             * :keyvalues={xrow01/xcolfam01:xcol01/1465885252556/Put
//             * /vlen=8/seqid=0,
//             * xrow01/xcolfam01:xcol02/1465885252556/Put/vlen=8/seqid=0,
//             * xrow01/xcolfam01:xcol03/1465885252556/Put/vlen=8/seqid=0}
//             * 2016-06-14 15:46:10,723 INFO [main] hbasetest.HBaseStudy:
//             * ======ScanAll
//             * :keyvalues={xrow02/xcolfam01:xcol01/1465887392414/Put
//             * /vlen=9/seqid=0,
//             * xrow02/xcolfam01:xcol02/1465887392414/Put/vlen=9/seqid=0}
//             * 2016-06-14 15:46:10,723 INFO [main] hbasetest.HBaseStudy:
//             * ======ScanAll
//             * :keyvalues={xrow03/xcolfam01:xcol01/1465887392428/Put
//             * /vlen=10/seqid=0,
//             * xrow03/xcolfam01:xcol02/1465887392428/Put/vlen=10/seqid=0,
//             * xrow03/xcolfam01:xcol03/1465887392428/Put/vlen=10/seqid=0}
//             * 2016-06-14 15:46:10,723 INFO [main] hbasetest.HBaseStudy:
//             * ======ScanAll
//             * :keyvalues={xrow04/xcolfam01:xcol01/1465887392432/Put
//             * /vlen=10/seqid=0,
//             * xrow04/xcolfam01:xcol02/1465887392432/Put/vlen=10/seqid=0,
//             * xrow04/xcolfam01:xcol03/1465887392432/Put/vlen=10/seqid=0,
//             * xrow04/xcolfam01:xcol04/1465887392432/Put/vlen=10/seqid=0}
//             */
//            logger.info("======ScanAll:" + resAll);
//        }
//        scannerAll.close();
// 
//        Scan scanColFam = new Scan();
//        scanColFam.addFamily(Bytes.toBytes("xcolfam01"));
//        ResultScanner scannerColFam = tbl.getScanner(scanColFam);
//        for (Result resColFam : scannerColFam) {
//            /*
//             * 2016-06-14 15:50:54,690 INFO [main] hbasetest.HBaseStudy:
//             * ======scannerColFam
//             * :keyvalues={xrow01/xcolfam01:xcol01/1465885252556
//             * /Put/vlen=8/seqid=0,
//             * xrow01/xcolfam01:xcol02/1465885252556/Put/vlen=8/seqid=0,
//             * xrow01/xcolfam01:xcol03/1465885252556/Put/vlen=8/seqid=0}
//             * 2016-06-14 15:50:54,690 INFO [main] hbasetest.HBaseStudy:
//             * ======scannerColFam
//             * :keyvalues={xrow02/xcolfam01:xcol01/1465887392414
//             * /Put/vlen=9/seqid=0,
//             * xrow02/xcolfam01:xcol02/1465887392414/Put/vlen=9/seqid=0}
//             * 2016-06-14 15:50:54,690 INFO [main] hbasetest.HBaseStudy:
//             * ======scannerColFam
//             * :keyvalues={xrow03/xcolfam01:xcol01/1465887392428
//             * /Put/vlen=10/seqid=0,
//             * xrow03/xcolfam01:xcol02/1465887392428/Put/vlen=10/seqid=0,
//             * xrow03/xcolfam01:xcol03/1465887392428/Put/vlen=10/seqid=0}
//             * 2016-06-14 15:50:54,690 INFO [main] hbasetest.HBaseStudy:
//             * ======scannerColFam
//             * :keyvalues={xrow04/xcolfam01:xcol01/1465887392432
//             * /Put/vlen=10/seqid=0,
//             * xrow04/xcolfam01:xcol02/1465887392432/Put/vlen=10/seqid=0,
//             * xrow04/xcolfam01:xcol03/1465887392432/Put/vlen=10/seqid=0,
//             * xrow04/xcolfam01:xcol04/1465887392432/Put/vlen=10/seqid=0}
//             */
//            logger.info("======scannerColFam:" + resColFam);
//        }
//        scannerColFam.close();
// 
//        Scan scanRow = new Scan();
//        scanRow.addColumn(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol02"))
//                .addColumn(Bytes.toBytes("xcolfam01"), Bytes.toBytes("xcol04")).setStartRow(Bytes.toBytes("xrow03"))
//                .setStopRow(Bytes.toBytes("xrow05"));
//        ResultScanner scannerRow = tbl.getScanner(scanRow);
//        for (Result resRow : scannerRow) {
//            /*
//             * 2016-06-14 15:57:29,449 INFO [main] hbasetest.HBaseStudy:
//             * ======scannerRow
//             * :keyvalues={xrow03/xcolfam01:xcol02/1465887392428/
//             * Put/vlen=10/seqid=0} 2016-06-14 15:57:29,449 INFO [main]
//             * hbasetest.HBaseStudy:
//             * ======scannerRow:keyvalues={xrow04/xcolfam01
//             * :xcol02/1465887392432/Put/vlen=10/seqid=0,
//             * xrow04/xcolfam01:xcol04/1465887392432/Put/vlen=10/seqid=0}
//             */
//            logger.info("======scannerRow:" + resRow);
//        }
//        scannerRow.close();
//    }
// 
//    public static void main(String[] args) {
//        HbaseStudy hb = new HbaseStudy();
//        try {
//            hb.createTable();
//            hb.putTableData();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
// 
//        try {
//            // hb.getTableData();
//            // hb.putTableDataRow();
//            hb.scanTableData();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}