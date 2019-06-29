package com.etl.possale;

import com.config.SaprkSessionUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import java.util.ArrayList;
import java.util.List;

public class Possale {

    public static final String EXTERNAL = "external_";

    public static void readAndSave(SqlAndTable sqlAndTable){
        String tableName = sqlAndTable.getTableName();
        String sql = sqlAndTable.getSql();
        SparkSession session = SaprkSessionUtil.getHiveSession();
        String table = tableName.substring(0,tableName.indexOf("."));
        String externalName = EXTERNAL +table;
       // session.sql("create   table if not exists " + externalName +" like "+tableName);

            for (int i =0;i < 2;i++){

            sql = sql.replaceAll("@",String.valueOf(i));

            Dataset<Row> dataset = session.sql(sql);
            dataset.write().mode(SaveMode.Append).insertInto("biz_dl_so_sale."+externalName);
           // dataset.show();
           // dataset.write().saveAsTable("biz_dl_so_sale."+externalName);

          session.sql("insert into "+tableName+" select * from " +tableName).show();



        }
    }

    public static void main (String []args){

        List<SqlAndTable> sqlAndTableList = new ArrayList<>();
        fillParam(sqlAndTableList);
        for (SqlAndTable sqlAndTable:sqlAndTableList){
             readAndSave(sqlAndTable);
        }
    }

    private static void fillParam(List<SqlAndTable> sqlAndTableList) {
        SqlAndTable sqlAndTable = new SqlAndTable();
        String  sql = "SELECT\n" + "\tsoorderdetail.Id AS id,\n" + "\tsoorderdetail.ProductId AS ProductId,\n"
                + "\tDATE_FORMAT(\n" + "\t\tsoorder.CreateTime,\n" + "\t\t'%Y-%m-%d'\n" + "\t) AS BizDate,\n"
                + "\tsoorder.CreateTime AS CreateTime,\n" + "\t1 AS BizType,\n" + "\t1 AS OrderType,\n"
                + "\tsoorder.SOId AS SOId,\n" + "\tsoorderdetail.TaxAmount AS TaxAmount,\n"
                + "\tsoorderdetail.UnTaxPrice AS UnTaxPrice,\n" + "\tsoorderdetail.OrderQty AS Qty,\n"
                + "\tsoorderdetail.GiftType AS GiftType,\n" + "\tsoorder.id AS BizFormId,\n"
                + "\tCASE IFNULL(soorder. CODE, '0')\n" + "WHEN '' THEN\n" + "\t'0'\n" + "ELSE\n"
                + "\tIFNULL(soorder. CODE, '0')\n" + "END AS BizFormCode,\n"
                + " soorderdetail.SalePrice AS SalePrice,\n" + " soorderdetail.TaxPrice AS TaxPrice,\n"
                + " soorderdetail.DiscountRate AS DiscountRate,\n" + " soorder.CreateEmpId AS CreateEmpId,\n"
                + " soorder.SaleEmpId AS SaleEmpId,\n"
                + " soorderdetail.OrderQty * soorderdetail.UnTaxPrice AS UnTaxAmount,\n"
                + " soorderdetail.AutoIncNo AS AutoIncNo\n" + "FROM\n"
                + "\tbiz_dl_so_sale.biz_tbl_soorder_detail_$i AS soorderdetail\n"
                + "LEFT JOIN biz_dl_so_sale.biz_tbl_soorder_$i AS soorder ON soorderdetail.orderId = soorder.id";
        sqlAndTable.setSql(sql);
        sqlAndTable.setTableName("biz_dl_so_sale.biz_tbl_possale");
        //sqlAndTableList.add(sqlAndTable);


        SqlAndTable test = new SqlAndTable();
        test.setSql("select * from biz_dl_so_sale.biz_tbl_soorder_detail_@ ");
        test.setTableName("biz_dl_so_sale.biz_dl_so_sale");
        test.setStructName("biz_dl_so_sale.biz_dl_so_sale");
        sqlAndTableList.add(test);

    }
}
