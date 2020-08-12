package cn.edu.ruc.common;

import java.util.List;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * 单条链接数据获取和插入mongo
 * @author huangzhen
 */
public class DataGetAndInsert extends Thread{
    //数据获取链接
    public String docSingleUrl;
    //数据获取参数
    public String docSingleParam;
    //梁斌接口data结构
    public String colName1;
    //mongo库名
    public String dbName;
    //mongo表名
    public String colName;

    public static final Logger log = Logger.getLogger(DataGetAndInsert.class);

    public DataGetAndInsert() {

    }

    public DataGetAndInsert(String docSingleUrl1,String docSingleParam1, String colName11, String dbName1, String colName12) {
        this.docSingleUrl = docSingleUrl1;
        this.docSingleParam = docSingleParam1;
        this.colName1 = colName11;
        this.dbName = dbName1;
        this.colName = colName12;
    }

    public void run() {
        List<Document> docs = DataGetUtils.getSingle(docSingleUrl, docSingleParam, colName1, dbName);
        if(docs==null || docs.isEmpty()) {
            log.warn(docSingleUrl + ", get empty data");
        }
        else {
            try {
                log.info(docSingleUrl + ",get data success.");
                MongoClient client = new MongoClient("localhost", 38018);
                MongoDatabase db = client.getDatabase(dbName);
                MongoCollection<Document> col = db.getCollection(colName);
                col.insertMany(docs);
                log.info(this.docSingleUrl + ", insert mongo success. " + dbName + ", " + colName );
                client.close();

            }
            catch(Exception e) {
                log.error(  this.docSingleUrl + ", insert mongo error. " + dbName + ", " + colName , e);
                EmailUtils.sendEmail(this.docSingleUrl + ", insert mongo error. " + dbName + ", " + colName + e.getMessage());
            }
        }
    }
}
