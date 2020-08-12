package cn.edu.ruc.datastore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import cn.edu.ruc.common.EmailUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class DataStore extends Thread {
    private static final Logger log = Logger.getLogger(DataStore.class);
    //归档库名
    public String dbName;
    //归档表名
    public String colName;
    //mongo保存记录数
    public Long keepNum;
    //一次归档数
    public Integer storeNum;
    //归档基数
    public Integer baseNum;
    //是否删除mongo数据
    public Boolean isDelete;

    public DataStore(){

    }
    public DataStore(String _dbName, String _colName, Long _keepNum, Integer _storeNum,  Integer _baseNum,Boolean _isDelete){
        this.dbName = _dbName;
        this.colName = _colName;
        this.keepNum = _keepNum;
        this.storeNum = _storeNum;
        this.baseNum = _baseNum;
        this.isDelete = _isDelete;
    }

    public void run(){
        try{
            //表示是否有归档数据
            Boolean hasDeleted = false;
            //数据归档目录
            String file_root_dir = "D://Data1/";
            MongoClient client = new MongoClient("localhost", 38018);
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> col = db.getCollection(colName);
            MongoCollection<Document> colRange = db.getCollection("range");
            MongoCollection<Document> colFile = db.getCollection("fileState");
            while(true){
                hasDeleted = false;
                FindIterable<Document> iter = colRange.find(new Document("colName", colName));
                MongoCursor<Document> cursor = iter.iterator();
                Document range = null;
                if(cursor.hasNext()){
                    range = cursor.next();
                }
                Long minid = new Long(range.getString("minid"));
                Long maxid = new Long(range.getString("maxid"));
                FindIterable<Document> iter1 = col.find().sort(new Document("_id",-1));
                MongoCursor<Document> cursor1 = iter1.iterator();
                if(cursor1.hasNext()) {
                    Document maxDoc = cursor1.next();
                    maxid = maxDoc.getLong("_id");

                }
                Gson gson = new Gson();
                //当前mongo数据库中存储数据条数
                Long ampNum = maxid - minid;
                //如果当前mongo数据库中条数取出storeNum条数据后，还有剩余keepNum，则进行归档
                if(ampNum > storeNum){
                    hasDeleted = true;
                    //归档排序字段
                    String sortKey = "crawler_time";
                    if("profile".equals(colName)){
                        sortKey = "crawler_date";
                    }
                    if("Weixin1".equals(dbName)){
                        sortKey = "ts";
                    }
                    if("profile2".equals(colName)) {
                        sortKey = "crawler_time_stamp";
                    }
                    if("Zhihu1".equals(dbName)) {
                        sortKey = "spider_time";
                    }
                    //计算归档目录
                    Long dirid = minid;
                    Long dir1 = dirid / (1000 * baseNum);
                    dirid = dirid % (1000 * baseNum);
                    Long dir2 = dirid / baseNum;
                    dir2 = dir2 - dir2 % (storeNum / baseNum);
                    Long tmpStartId = dir1 * 1000 * baseNum + dir2 * baseNum;
                    String file_dir = file_root_dir
                            + "/" + dbName + "/" + colName + "/"
                            + dir1 + "/";
                    File f = new File(file_dir);
                    Boolean isMkdirSuccess = f.mkdirs();
                    Long newMinid = null, minid1 = null, maxid1 = null;
                    String mintime1="", maxtime1 = "";
                    String storedir = dbName + "/" + colName + "/"
                            + dir1 + "/" + dir2;
                    Boolean isWriteFileSuccess = false;
                    try {
                        File f1 = new File(file_dir + dir2 + ".txt");
                        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f1),"UTF-8"));
                        iter = col.find(new Document("_id", new Document("$gte", minid).append("$lt", minid + storeNum))).sort(new Document("_id", 1));
                        cursor = iter.iterator();
                        //将数据进行归档
                        while(cursor.hasNext()){
                            Document doc = cursor.next();
                            newMinid = doc.getLong("_id");
                            if(minid1 == null){
                                minid1 = newMinid;
                                if("profile2".equals(colName)) {
                                    mintime1 = doc.getString("crawler_date");
                                }
                                else {
                                    mintime1 = doc.getString(sortKey);
                                }
                            }
                            maxid1 = newMinid;
                            if("profile2".equals(colName)) {
                                maxtime1 = doc.getString("crawler_date");
                            }
                            else {
                                maxtime1 = doc.getString(sortKey);
                            }
                            bw.write(gson.toJson(doc));
                            bw.newLine();
                        }
                        bw.close();
                        //如果这个id范围内有数据
                        if(minid1!=null){
                            if("Weibo1".equals(dbName) && (!"profile2".equals(colName))){
                                mintime1 = mintime1.split(" ")[0];
                                maxtime1 = maxtime1.split(" ")[0];
                            }
                            else if("Weixin1".equals(dbName)){
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                Long dateLong = new Long(mintime1) * 1000l;
                                Date dateObj = new Date(dateLong);
                                mintime1 = sdf.format(dateObj);
                                dateLong = new Long(maxtime1) * 1000l;
                                dateObj = new Date(dateLong);
                                maxtime1 = sdf.format(dateObj);
                            }
                            else if("profile2".equals(colName)) {
                                mintime1 = mintime1;
                                maxtime1 = maxtime1;
                            }
                            log.info(dbName + " " + colName +" : " + "Write file : "+ file_dir + "/" + dir2 + ".txt success.");
                            isWriteFileSuccess = true;
                        }
                    }
                    catch (Exception e) {
                        log.error(dbName + " " + colName +" : " + "Write file : "+ file_dir + "/" + dir2 + ".txt error.", e);
                        EmailUtils.sendEmail(dbName + " " + colName +" : " + "Write file : "+ file_dir + "/" + dir2 + ".txt error. " + e.getStackTrace());
                        break;
                    }
                    //对数据进行压缩
                    Boolean isCompressSuccess = false;
                    try{
                        File f2 = new File(file_dir + dir2 + ".txt");
                        InputStreamReader fs = new InputStreamReader(new FileInputStream(f2), "UTF-8");
                        BufferedReader bs = new BufferedReader(fs);
                        FileOutputStream outfile = new FileOutputStream(file_dir + dir2 + ".txt.xz");
                        LZMA2Options options = new LZMA2Options();
                        options.setPreset(3);
                        XZOutputStream out = new XZOutputStream(outfile, options);
                        String line = null;
                        while((line = bs.readLine())!=null) {
                            line += "\r\n";
                            out.write(line.getBytes());
                        }
                        bs.close();
                        fs.close();
                        out.close();
                        Boolean isF2Deleted = f2.delete();
                        log.info(dbName + " " + colName +" : " + "Compress "+ minid + " , " + storeNum + " to file success.");
                        isCompressSuccess = true;
                    }
                    catch(Exception e){
                        log.error(dbName + " " + colName +" : " + "Compress "+ minid + " , " + storeNum + " to file error.", e);
                        EmailUtils.sendEmail(dbName + " " + colName +" : " + "Compress "+ minid + " , " + storeNum + " to file error." + e.getStackTrace());
                        break;
                    }

                    //更新归档信息
                    //更新归档信息到mongo库
                    Document newFileState = new Document("dir", storedir).append("minid", minid1)
                            .append("mintime", mintime1)
                            .append("maxid", maxid1)
                            .append("maxtime", maxtime1);
                    //更新归档信息到文件系统
                    try{
                        File f0 = new File(file_dir  + "mapping.txt");
                        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f0, true),"UTF-8"));
                        bw.write(gson.toJson(newFileState));
                        bw.newLine();
                        bw.close();
                    }
                    catch(Exception e0){
                        log.error(dbName + " " + colName +" :  store mapping record error. " +   gson.toJson(newFileState));
                        EmailUtils.sendEmail(dbName + " " + colName +" :  store mapping record error. " +   gson.toJson(newFileState));
                    }
                    colFile.insertOne(newFileState);
                    Long newStoreMinid = minid + storeNum;
                    colRange.updateOne(new Document("colName",colName), new Document("$set", new Document("minid", newStoreMinid.toString())));
                    //删除mongo数据,删除比maxid-keepNum小的数据
                    if(isDelete && isCompressSuccess && isWriteFileSuccess){
                        Long deleteNum = minid + storeNum - keepNum;
                        col.deleteMany(new Document("_id", new Document("$lt", deleteNum)));
                        colRange.updateOne(new Document("colName",  colName), new Document("$set", new Document("keepMinid", deleteNum)));
                        log.info(dbName + " " + colName +" : " + "Delete $lt"+ deleteNum + " , " + storeNum + " success.");
                    }

                }
                if(!hasDeleted){
                    try {
                        Thread.sleep(60000);
                        log.info(dbName + " " + colName +" : Thread cleep 60s.");
                    } catch (InterruptedException e) {
                        log.error( dbName + " " + colName +" : " + "Thread sleep error." ,e);
                        EmailUtils.sendEmail(dbName + " " + colName +" : " + "Thread sleep error." + e.getStackTrace());
                    }
                }
            }
        }
        catch(Exception e0){
            EmailUtils.sendEmail(dbName + " " + colName +" : DataStore error." + e0.getStackTrace());
            log.error(dbName + " " + colName +" : DataStore error." , e0 );
        }
        log.warn(dbName + " " + colName + " : DataStore stoped.");
    }

    public static void main(String[] args){

        PropertyConfigurator.configure("datastore.log4j.properties");
        //各数据表开启归档线程
        Thread t_weibo = new DataStore("Weibo1", "weibo", 20000000l, 1000000 ,1000000, true);
        t_weibo.start();
        Thread t_cwb = new DataStore("Weibo1", "cwb", 200000l, 10000,10000 , true);
        t_cwb.start();
        Thread t_profile = new DataStore("Weibo1", "profile", 1000000l, 1000000, 1000000 , true);
        t_profile.start();
        Thread t_tag = new DataStore("Weibo1", "tag", 1000000l, 100000 ,100000, true);
        t_tag.start();
        Thread t_comment = new DataStore("Weibo1", "comment", 100000l, 100000, 100000 , true);
        t_comment.start();
        Thread t_profile_2 = new DataStore("Weibo1", "profile2", 100000l, 1000000, 1000000 , true);
        t_profile_2.start();
        Thread t_locate = new DataStore("Weibo1", "locate", 100000l, 1000000, 1000000 , true);
        t_locate.start();
        Thread t_bangdan = new DataStore("Weibo1", "bangdan", 10000l, 10000, 10000 , true);
        t_bangdan.start();
        Thread t_huati = new DataStore("Weibo1", "huati", 100000l, 100000, 100000 , true);
        t_huati.start();
        Thread t_huati_bang = new DataStore("Weibo1", "huati_bang", 10000l, 10000, 10000 , true);
        t_huati_bang.start();

        Thread t_biz = new DataStore("Weixin1", "biz", 1000000l, 100000, 100000 , true);
        t_biz.start();
        Thread t_page = new DataStore("Weixin1", "page", 100000l, 10000, 10000, true);
        t_page.start();
        Thread t_click = new DataStore("Weixin1", "click", 2000000l, 1000000,1000000 , true);
        t_click.start();
        Thread t_comment1 = new DataStore("Weixin1", "comment", 200000l, 100000,100000 , true);
        t_comment1.start();

        Thread t_question_comment = new DataStore("Zhihu1", "question_comment", 200000l, 100000, 100000 , true);
        t_question_comment.start();
        Thread t_question = new DataStore("Zhihu1", "question", 200000l, 100000, 100000 , true);
        t_question.start();
        Thread t_comment2 = new DataStore("Zhihu1", "comment", 2000000l, 1000000, 1000000 , true);
        t_comment2.start();
        Thread t_answer = new DataStore("Zhihu1", "answer", 200000l, 100000, 100000 , true);
        t_answer.start();
        Thread t_user = new DataStore("Zhihu1", "user", 200000l, 100000, 100000 , true);
        t_user.start();
    }
}
