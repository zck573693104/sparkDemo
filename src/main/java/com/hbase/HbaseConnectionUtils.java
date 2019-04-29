package com.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
 

 
public class HbaseConnectionUtils {
    private static Logger log = Logger.getLogger(HbaseConnectionUtils.class);
    private static HbaseConnectionUtils instance = null;
    public static Configuration config = null;
    private static Connection connection = null;
 
    private HbaseConnectionUtils() {
 
    }
 
    /**
     * @author c_lishaoying 983068303@qq.com
     *
     *         加载集群配置
     */
    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "lcoalhost");
        config.set("hbase.zookeeper.property.clientPort", "");
        config.setLong("hbase.client.scanner.timeout.period", Integer.MAX_VALUE);
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (Exception e) {
            log.debug("connection构建失败");
        }
    }
 
    public static HbaseConnectionUtils getInstance() {
        if (instance == null) {
            // 给类加锁 防止线程并发
            synchronized (HbaseConnectionUtils.class) {
                if (instance == null) {
                    instance = new HbaseConnectionUtils();
                }
            }
        }
        return instance;
    }
 
    public Configuration getConfiguration() {
        return config;
    }
 
    public Connection getConnection() {
        return connection;
    }
 
    /**
     * @author c_lishaoying 983068303@qq.com 获取表连接
     *
     */
    public static Table getTable(String tableName) {
        try {
            return connection.getTable(TableName.valueOf(tableName));
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
        AggregationClient aggregationClient = new AggregationClient(config);
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
            log.info("配置的表名称为空！");
        } else {
            log.info("表名称为:" + tablename);
            Table queryTablename = getTable(tablename);
            Scan scan = new Scan();
            scan = getScan(queryString);
            count = getTotalRecord(queryTablename, config, scan);
        }
 
        return count;
    }
 
    /**
     * @author c_lishaoying
     * @email 983068303@qq.com 非协处理器查询总数据 添加查询条件的Scan统计总数 queryString数组长度为3
     *        queryString[0] 列族 queryString[1]字段 queryString[2] 字段值
     */
    public static int scan(String tablename, String[] condition) {
        Table queryTablename = getTable(tablename);
        String[] s = condition;
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL,
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
     * @author c_lishaoying
     * @email 983068303@qq.com 查询字段相等的值 相当于where city = ‘上海’ queryString数组长度为3
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
                Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes.toBytes(s[2])));
        scan.setFilter(filterList);
        return scan;
    }
 
    /**
     * @author c_lishaoying
     * @email 983068303@qq.com 查询字段相等的值 应用正则表达式 相当于where city like ‘%上海%’
     *        queryString数组长度为3 queryString[0] 列族 queryString[1]字段
     *        queryString[2] 字段值
     */
    public static Scan regexscan(String tablename, String[] condition) {
        Table queryTablename = getTable(tablename);
        String[] s = condition;
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL,
                new RegexStringComparator(".*" + s[2] + ".*"));
        scan.setFilter(singleColumnValueFilter);
        return scan;
    }
 
    /**
     * @author c_lishaoying
     * @email 983068303@qq.com 查询该列的值
     */
    public static Scan valuescan(String tablename, String[] condition) {
        Table queryTablename = getTable(tablename);
        String[] s = condition;
        Scan scan = new Scan();
 
        Filter filter1 = new ValueFilter(CompareOp.EQUAL,
                new SubstringComparator(s[2]));
        scan.setFilter(filter1);
        return scan;
    }
 
    /**
     * @author c_lishaoying
     * @email 983068303@qq.com 根据rowkey 查询 相当于 where id in （）
     *        queryString数组都为rowkey
     */
    public static Scan rowscan(String tablename, String[] condition) {
        Scan scan = new Scan();
        FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
        for (String s : condition) {
            Filter filter = new RowFilter(CompareOp.EQUAL,
                    new SubstringComparator(s));
            filterList.addFilter(filter);
        }
        scan.setFilter(filterList);
        return scan;
    }
 
    /**
     * @author c_lishaoying
     * @email 983068303@qq.com 查询该列的值 相当于where city = ‘上海’ AND name =‘酒店’
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
                    CompareOp.EQUAL, Bytes.toBytes(s[2]))); // 值
        }
        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, filters);
        scan.setFilter(filterList);
        return scan;
    }
 
    /**
     * @author c_lishaoying
     * @email 983068303@qq.com 查询该列的值 相当于where city = ‘上海’ OR name =‘酒店’
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
                    CompareOp.EQUAL, Bytes.toBytes(s[2]));
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
     * @author c_lishaoying
     * @email 983068303@qq.com 通过多条件联合查询和限制返回页数 相当于mysql 中的limit 0,1000
     */
    public Scan queryByFilter(String tablename, List<String[]> arr,
            String starString, String stopString) throws IOException {
        FilterList filterList = new FilterList();
        Scan scan = new Scan();
        for (String[] s : arr) {
            SubstringComparator comp = new SubstringComparator(s[2]);
            filterList
                    .addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]),
                            Bytes.toBytes(s[1]), CompareOp.EQUAL, comp));
        }
        PageFilter pageFilter = new PageFilter(1000);
        filterList.addFilter(pageFilter);
        scan.setFilter(filterList);
        scan.setStartRow(Bytes.toBytes(starString));
        scan.setStartRow(Bytes.toBytes(stopString));
        return scan;
    }
 
}