package com.hbase;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
 
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.common.StatsSetupConst.StatDB.hbase;


public class SQLAnalysisUtils {
 
    /**
     * 通过输入的SQL语句获取插入表名与插入的列
     * @param sqlStr
     * @return
     * @throws JSQLParserException
     */
//    public static Map getSQLPares(String sqlStr) throws JSQLParserException {
//        //解析SQL后把要的内容存储到MAP里
//        Map sqlPareMap = new HashMap();
//        //生成对象
//        CCJSqlParserManager pm = new CCJSqlParserManager();
//        //返回一个InsertStatement对象
//        System.out.println("sqlStr ================ " + sqlStr);
//        Insert insertStatement = (Insert) pm.parse(new StringReader(sqlStr));
//        //返回要插入的目标表表名
//        String insertTableName=insertStatement.getTable().getName();
//        //放入MAP里
//        sqlPareMap.put("tgtTableName",insertTableName);
//        //通过目标表名得到字段名
//        List<String> tgtTableColumnList = HBaseUtils.getTableColumn(insertTableName);
//        //如果目标表为空字段名直接从SQL语句里取得
//        if(tgtTableColumnList.size()==0||tgtTableColumnList==null){
//            tgtTableColumnList = getColumnName(insertStatement);
//        }
//        //把返回的列名LIST放入MAP里
//        sqlPareMap.put("tgtTableColumn", tgtTableColumnList);
//        //把insert语句后面跟着的SELECT语句放到MAP里
//        sqlPareMap.put("SQL",insertStatement.getSelect().toString());
//        //返回一个查询对象
//        Select selectStatement = (Select) pm.parse(new StringReader(insertStatement.getSelect().toString()));
//        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
//        //获取查询对象中源表的表名LIST
//        List<String> tableNameList = tablesNamesFinder.getTableList(selectStatement);
//        //放入到MAP里
//        sqlPareMap.put("srcTableName",tableNameList);
//        return sqlPareMap;
//    }
 
    /**
     * 返回SQL语句中INSERT后面的字段名
     * @param insertStatement
     * @return
     * @throws JSQLParserException
     */
    public static List<String> getColumnName(Insert insertStatement) throws JSQLParserException {
        List<String> columnNameList = new ArrayList<String>();
        List<Column> columns=insertStatement.getColumns();
        for(Column column:columns){
            System.out.println("tableColumn=============="+column.getColumnName());
            columnNameList.add(column.getColumnName());
        }
        return columnNameList;
    }
 
}