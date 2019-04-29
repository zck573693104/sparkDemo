package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparkHBaseUtils {

    private static final String QUORUM = "192.168.0.100";
    private static final String CLIENTPORT = "2181";
    private static Configuration conf = null;
    private static Connection conn = null;
    private static JavaSparkContext context = null;

    /**
     * 返回一个JavaSparkContext
     *
     * @return
     */
    public static synchronized JavaSparkContext getJavaSparkContext() {
        if (context == null) {
            SparkConf sparkConf = new SparkConf();
            sparkConf.setAppName("SparkApp");
            sparkConf.setMaster("local[5]");
            JavaSparkContext context = new JavaSparkContext(sparkConf);
        }
        return context;
    }


    /**
     * 获取全局唯一的Configuration实例
     *
     * @return
     */
    public static synchronized Configuration getConfiguration() {
        if (conf == null) {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", CLIENTPORT);
            conf.addResource("hbase-site.xml");
            conf.addResource("core-site.xml");
        }
        return conf;
    }

    /**
     * 获取全局唯一的HConnection实例
     *
     * @return
     */
    public static synchronized Connection getHConnection() {

        if (conf == null) {
            getConfiguration();
        }

        if (conn == null) {
            /*
             * * 创建一个HConnection
             * HConnection connection = HConnectionManager.createConnection(conf);
             * HTableInterface table = connection.getTable("mytable");
             * table.get(...); ...
             * table.close();
             * connection.close();
             * */
            try {
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return conn;
    }


    // 关闭连接
    public static void connClose() {
        try {
            if (null != conn)
                conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Table getTable(String tableName) {
        try {
            return getHConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @author c_lishaoying
     * @email 983068303@qq.com 协处理器查询总数据
     *
     */
    public int getRecordCount(String tableName) {
        AggregationClient aggregationClient = new AggregationClient(getConfiguration());
        Scan scan = new Scan();
        try {
            Long rowCount = aggregationClient.rowCount(
                    TableName.valueOf(tableName), new LongColumnInterpreter(),
                    scan);
            aggregationClient.close();
            return rowCount.intValue();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * @author c_lishaoying
     * @email 983068303@qq.com 协处理器查询总数据 添加查询条件的Scan统计总数
     */
    public static int getTotalRecord(Table keyIndexTable, Configuration config,
                                     final Scan scan) {
        int count = 0;
        AggregationClient aggregationClient = new AggregationClient(config);
        try {
            Long rowCount = aggregationClient.rowCount(keyIndexTable,
                    new LongColumnInterpreter(), scan);
            aggregationClient.close();
            count = rowCount.intValue();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return count;
    }

    /**
     * @author c_lishaoying
     * @email 983068303@qq.com 协处理器查询总数据 添加查询条件的Scan统计总数 queryString数组长度为3
     *        queryString[0] 列族 queryString[1]字段 queryString[2] 字段值
     */
    public static int queryByCloumn(String tablename, String[] queryString) {

        int count = 0;
        if (tablename == null) {
           throw new RuntimeException();
        } else {
            Table queryTablename = getTable(tablename);
            Scan scan = new Scan();
            scan = getScan(queryString);
            count = getTotalRecord(queryTablename, getConfiguration(), scan);
        }

        return count;
    }

    /**
     *        非协处理器查询总数据 添加查询条件的Scan统计总数 queryString数组长度为3
     *        queryString[0] 列族 queryString[1]字段 queryString[2] 字段值
     */
    public static int scan(String tablename, String[] condition) {
        Table queryTablename = getTable(tablename);
        String[] s = condition;
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(s[0]), Bytes.toBytes(s[1]), CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(s[2])));
        scan.setFilter(singleColumnValueFilter);
        int i = 0;
        try {
            ResultScanner rs = queryTablename.getScanner(scan);
            /*
             * for(Iterator it = rs.iterator(); it.hasNext();){ it.next(); }
             */
            for (Result r : rs) {
                i++;
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return i;
    }

    /**
     *        查询字段相等的值 相当于where city = ‘上海’ queryString数组长度为3
     *        queryString[0] 列族 queryString[1]字段 queryString[2] 字段值
     */
    public static Scan getScan(String[] condition) {
        Scan scan = new Scan();
        if (condition == null || condition.length != 3) {
            return new Scan();
        }
        FilterList filterList = new FilterList();
        String[] s = condition;
        BinaryComparator comp = new BinaryComparator(Bytes.toBytes(s[2]));
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]),
                Bytes.toBytes(s[1]), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(s[2])));
        scan.setFilter(filterList);
        return scan;
    }

    /**
              查询字段相等的值 应用正则表达式 相当于where city like ‘%上海%’
     *        queryString数组长度为3 queryString[0] 列族 queryString[1]字段
     *        queryString[2] 字段值
     */
    public static Scan regexscan(String tablename, String[] condition) {
        Table queryTablename = getTable(tablename);
        String[] s = condition;
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(s[0]), Bytes.toBytes(s[1]), CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(".*" + s[2] + ".*"));
        scan.setFilter(singleColumnValueFilter);
        return scan;
    }

    /**
     *  查询该列的值
     */
    public static Scan valuescan(String tablename, String[] condition) {
        Table queryTablename = getTable(tablename);
        String[] s = condition;
        Scan scan = new Scan();

        Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(s[2]));
        scan.setFilter(filter1);
        return scan;
    }

    /**
     *      根据rowkey 查询 相当于 where id in （）
     *        queryString数组都为rowkey
     */
    public static Scan rowscan(String tablename, String[] condition) {
        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (String s : condition) {
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(s));
            filterList.addFilter(filter);
        }
        scan.setFilter(filterList);
        return scan;
    }

    /**
     *        查询该列的值 相当于where city = ‘上海’ AND name =‘酒店’
     *        queryString数组长度为3 queryString[0] 列族 queryString[1]字段
     *        queryString[2] 字段值
     */

    public static Scan listAndColumnscan(String tablename,
                                         List<String[]> condition) {
        Scan scan = new Scan();
        List<Filter> filters = new ArrayList<Filter>();

        for (String[] s : condition) {
            filters.add(new SingleColumnValueFilter(Bytes.toBytes(s[0]), // 列族
                    Bytes.toBytes(s[1]), // 列名
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes(s[2]))); // 值
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        scan.setFilter(filterList);
        return scan;
    }

    /**
              查询该列的值 相当于where city = ‘上海’ OR name =‘酒店’
     *        queryString数组长度为3 queryString[0] 列族 queryString[1]字段
     *        queryString[2] 字段值
     */
    public static Scan listOrColumnscan(String tablename,
                                        List<String[]> condition) {
        Scan scan = new Scan();
        ArrayList<Filter> listForFilters = new ArrayList<Filter>();
        Filter filter = null;
        for (String[] s : condition) {
            filter = new SingleColumnValueFilter(Bytes.toBytes(s[0]), // 列族
                    Bytes.toBytes(s[1]), // 列名
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes(s[2]));
            listForFilters.add(filter);
        }
        // 通过将operator参数设置为Operator.MUST_PASS_ONE,达到list中各filter为"或"的关系
        // 默认operator参数的值为Operator.MUST_PASS_ALL,即list中各filter为"并"的关系
        Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE,
                listForFilters);
        scan.setFilter(filterList);// 多条件过滤
        return scan;
    }

    /**
     *  通过多条件联合查询和限制返回页数 相当于mysql 中的limit 0,1000
     */
    public Scan queryByFilter(String tablename, List<String[]> arr,
                              String starString, String stopString) throws IOException {
        FilterList filterList = new FilterList();
        Scan scan = new Scan();
        for (String[] s : arr) {
            SubstringComparator comp = new SubstringComparator(s[2]);
            filterList
                    .addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]),
                            Bytes.toBytes(s[1]), CompareFilter.CompareOp.EQUAL, comp));
        }
        PageFilter pageFilter = new PageFilter(1000);
        filterList.addFilter(pageFilter);
        scan.setFilter(filterList);
        scan.setStartRow(Bytes.toBytes(starString));
        scan.setStartRow(Bytes.toBytes(stopString));
        return scan;
    }

}