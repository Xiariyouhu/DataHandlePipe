package cn.edu.ruc.dataget;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import cn.edu.ruc.common.DataGetUtils;
import cn.edu.ruc.common.EmailUtils;
import cn.edu.ruc.dto.Range;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * 数据获取同步处理类
 * @author huangzhen
 */
public class DataGetSync extends Thread {
    //开始获取id
    public Long startid;
    //连接数据库名
    public String dbName;
    //连接数据表名
    public String colName;
    //获取数据地址端口号
    public Integer port;
    //一次数据获取条数
    public Integer onceNum;
    //随机生成器
    public Random random;
    public static final Logger log = Logger.getLogger(DataGetSync.class);
    public DataGetSync() {
    }
    public DataGetSync(Long _startid, String _dbName,String _colName, Integer _port, Integer _onceNum) {
        this.random = new Random();
        this.startid = _startid;
        this.dbName = _dbName;
        this.colName = _colName;
        this.port = _port;
        this.onceNum = _onceNum;
    }

    public void run() {
        try{
            //连接数据库
            MongoClient client = new MongoClient("localhost", 38018);
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> col = db.getCollection(colName);
            MongoCollection<Document> colId = db.getCollection("startId");
            MongoCollection<Document> colRange = db.getCollection("range");
            String[] ips = {"101.236.39.7","101.236.61.188","101.236.60.121"};
            if(true){
                //每次获取数据前随机取ip
                int randInt = random.nextInt(3);
                String rangeUrl = "http://" + "dzc.pullword.com" +":"+port+"/"+ colName +"/range";
                //因为微博接口调整，为了保持数据库统一进行替换操作
                if("weibo".equals(colName)) {
                    rangeUrl = rangeUrl.replace("weibo", "weibo2");
                }
                //知乎数据从固定接口获取
                if("Zhihu1".equals(dbName)) {
                    rangeUrl = "http://" + "zhihu.pullword.com" +":"+port+"/"+ colName +"/range";
                }
                String rangeParam = "auth_usr=douzc";
                Range range = DataGetUtils.rangeGet(rangeUrl, rangeParam);
                if(startid == null){
                    log.error(dbName + " " + colName + " startid is null ");
                    EmailUtils.sendEmail(dbName + " " + colName + " startid is null ");
                }
                if(range==null || range.minid == null || range.maxid == null){
                    range = DataGetUtils.rangeGet(rangeUrl, rangeParam);
                    log.warn(dbName + " " + colName + " range is illegal : " + range);
                }
                //如果取数开始id已经小于range范围，则调整开始取数id
                Long nextid = startid < range.minid ? range.minid : startid;
                int j = 0;
                Long rangeMaxid = range.maxid;
                //上一次成功取数时间
                Date lastFetchTime = new Date();
                while(true){
                    randInt = random.nextInt(3);
                    String temp_ip = DataGetUtils.ipGet(0);
                    //拼接取数链接
                    String docSingleUrl = "http://"+ temp_ip +":"+port+"/"+ colName +"/" + nextid;
                    if("Zhihu1".equals(dbName)) {
                        docSingleUrl = "http://" + "zhihu.pullword.com" +":"+port+"/"+ colName +"/" + nextid;
                    }
                    if("weibo".equals(colName)) {
                        docSingleUrl = docSingleUrl.replace("weibo", "weibo2");
                    }
                    String docSingleParam = "auth_usr=douzc";
                    String colName1 = colName;
                    if("Weixin1".equals(dbName)){
                        colName1 += "s";
                    }
                    if("Zhihu1".equals(dbName)) {
                        colName1 = "data";
                    }
                    if("weibo".equals(colName)) {
                        colName1 = "weibo2";
                    }
                    if(true){
                        List<Document> docs = DataGetUtils.getSingle(docSingleUrl, docSingleParam, colName1, dbName);
                        //如果没有取到数，则sleep 2秒，重新取数
                        if(docs!=null && docs.isEmpty()){
                            //如果当前取数低于minid，调整为minid开始取数
                            range = DataGetUtils.rangeGet(rangeUrl, rangeParam);
                            if(range!=null && nextid < range.minid) {
                                nextid = range.maxid;
                                log.error(dbName + " "+ colName + " : nextid lower than lb's minid");
                                EmailUtils.sendEmail(dbName + " "+ colName + " : nextid lower than lb's minid");
                                continue;
                            }

                            try {
                                Thread.sleep(500);
                                log.info(dbName + " "+ colName + " : Thread sleep 500ms");
                            } catch (InterruptedException e) {
                                log.error(dbName + " "+ colName + " : Thread sleep error.", e);
                                EmailUtils.sendEmail(dbName + " "+ colName +" : Thread sleep error.");
                            }
                            Date nowTime = new Date();
                            Long splitTime = nowTime.getTime() - lastFetchTime.getTime();
                            //设置数据未更新警报时间：weibos和pages十分钟，其余12小时
                            Long threshold = 10l * 60l * 1000l;
                            if(!("weibos".equals(colName1) || "pages".equals(colName1))){
                                threshold *= 6l * 12l;
                            }
                            if(splitTime > threshold){
                                Range range1 = DataGetUtils.rangeGet(rangeUrl, rangeParam);
                                if(range1 == null){
                                    log.error(dbName + " "+ colName + " : failed to get range.");
                                    EmailUtils.sendEmail(dbName + " "+ colName + " : failed to get range.");
                                }

                                if(range1!=null &&( nextid > (range1.maxid + 2000) || nextid < range1.minid)){
                                    log.error(dbName + " "+ colName + " : nextid beyond of range.");
                                    EmailUtils.sendEmail(dbName + " "+ colName + " : nextid beyond of range.");

                                    if(nextid < range1.minid) {
                                        nextid = range1.maxid;
                                    }
                                }
                                lastFetchTime = new Date();
                            }

                            continue;
                        }

                        lastFetchTime = new Date();
                        //如果返回为null，则表示取数失败，发送警告
                        if(docs==null){
                            log.error(dbName + " "+ colName +" fetch data failed. Startid : " + nextid );
                            EmailUtils.sendEmail(dbName + " "+ colName +" fetch data failed. Startid : " + nextid );
                        }

                        try{
                            if(docs!= null &&(! docs.isEmpty())){
                                col.insertMany(docs);
                            }
                            else{
                                log.warn(dbName + " "+ colName +" insert doc is null. Startid : " + nextid);
                            }
                        }
                        catch(Exception e){
                            log.error(dbName + " "+ colName +" insert mongo error. Startid : " + nextid, e);
                            EmailUtils.sendEmail(dbName + " "+ colName +" insert mongo error. Startid : " + nextid + e.getStackTrace());
                        }
                        nextid += onceNum;
                    }
                    //更新mongo获取数据状态
                    Document doc = new Document("colName", colName).append("startId", nextid);
                    colId.updateMany(Filters.eq("colName", colName), new Document("$set",doc));
                    colRange.updateMany(Filters.eq("colName",colName), new Document("$set",new Document("maxid",nextid.toString())));
                }
            }
        }
        catch(Exception e0){
            EmailUtils.sendEmail(dbName + " " + colName +" : DataGet error." + e0.getStackTrace());
            log.error(dbName + " " + colName +" : DataGet error." , e0 );
        }
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("dataget.log4j.properties");
        log.info("DataGet start");
        //获取开始取数id
        Long weiboid = 0l, profileid = 0l, cwbid = 0l, commentid = 0l, tagid = 0l, profile2id=0l, locateid=0l, bangdanid=0l,huatiid=0l,huati_bangid=0l,dianzanid=0l,remenid=0l;
        Long bizid = 0l, pageid = 0l , clickid = 0l, commentid1 = 0l;
        Long question_comment_id = 0l, question_id = 0l, answer_id = 0l, user_id = 0l, commentid2 = 0l;
        MongoClient client = new MongoClient("localhost", 38018);
        MongoDatabase db = client.getDatabase("Weibo1");
        MongoCollection<Document> col = db.getCollection("startId");
        Map<String, Long> startIds = new HashMap<String, Long>();
        FindIterable<Document> iter = col.find();
        MongoCursor<Document> cursor = iter.iterator();
        while(cursor.hasNext()) {
            Document doc = cursor.next();
            startIds.put(doc.getString("colName"), doc.getLong("startId"));
        }
        weiboid = startIds.get("weibo");
        profileid = startIds.get("profile");
        cwbid = startIds.get("cwb");
        commentid = startIds.get("comment");
        tagid = startIds.get("tag");
        profile2id = startIds.get("profile2");
        locateid = startIds.get("locate");
        bangdanid = startIds.get("bangdan");
        huatiid = startIds.get("huati");
        huati_bangid = startIds.get("huati_bang");
        dianzanid = startIds.get("dianzan");
        remenid = startIds.get("remen");
        MongoDatabase db1 = client.getDatabase("Weixin1");
        MongoCollection<Document> col1 = db1.getCollection("startId");
        Map<String, Long> startIds1 = new HashMap<String, Long>();
        FindIterable<Document> iter1 = col1.find();
        MongoCursor<Document> cursor1 = iter1.iterator();
        while(cursor1.hasNext()) {
            Document doc = cursor1.next();
            startIds1.put(doc.getString("colName"), doc.getLong("startId"));
        }
        pageid = startIds1.get("page");
        bizid = startIds1.get("biz");
        clickid = startIds1.get("click");
        commentid1 = startIds1.get("comment");

        MongoDatabase db2 = client.getDatabase("Zhihu1");
        MongoCollection<Document> col2 = db2.getCollection("startId");
        Map<String, Long> startIds2 = new HashMap<String, Long>();
        FindIterable<Document> iter2 = col2.find();
        MongoCursor<Document> cursor2 = iter2.iterator();
        while(cursor2.hasNext()) {
            Document doc = cursor2.next();
            startIds2.put(doc.getString("colName"), doc.getLong("startId"));
        }
        question_comment_id = startIds2.get("question_comment");
        question_id = startIds2.get("question");
        commentid2 = startIds2.get("comment");
        answer_id = startIds2.get("answer");
        user_id = startIds2.get("user");

        //各接口单独线程取数
        Thread t_weibo = new DataGetSync(weiboid,"Weibo1", "weibo",22345, 1000);
        t_weibo.start();
        Thread t_profile = new DataGetSync(profileid,"Weibo1" ,"profile", 22345, 100);
        t_profile.start();
        Thread t_comment = new DataGetSync(commentid,"Weibo1","comment",22345,100);
        t_comment.start();
        Thread t_cwb = new DataGetSync(cwbid,"Weibo1","cwb",22345, 100);
        t_cwb.start();
        Thread t_tag = new DataGetSync(tagid, "Weibo1","tag",22345,100);
        t_tag.start();
        Thread t_profile_2 = new DataGetSync(profile2id, "Weibo1","profile2",22345,100);
        t_profile_2.start();
        Thread t_locate = new DataGetSync(locateid, "Weibo1","locate",22345,100);
        t_locate.start();
        Thread t_bangdan = new DataGetSync(bangdanid, "Weibo1","bangdan",22345,10);
        t_bangdan.start();
        Thread t_huati = new DataGetSync(huatiid, "Weibo1","huati",22345,100);
        t_huati.start();
        Thread t_huati_bang = new DataGetSync(huati_bangid, "Weibo1","huati_bang",22345,100);
        t_huati_bang.start();
        Thread t_dianzan = new DataGetSync(dianzanid, "Weibo1","dianzan",22345,100);
        t_dianzan.start();
        Thread t_remen = new DataGetSync(remenid, "Weibo1","remen",22345,100);
        t_remen.start();

        Thread t_biz = new DataGetSync(bizid, "Weixin1", "biz", 12345  ,100);
        t_biz.start();
        Thread t_page = new DataGetSync(pageid, "Weixin1", "page", 12345, 100);
        t_page.start();
        Thread t_click = new DataGetSync(clickid, "Weixin1", "click", 12345, 100);
        t_click.start();
        Thread t_comment1 = new DataGetSync(commentid1, "Weixin1", "comment", 12345, 100);
        t_comment1.start();


        Thread t_question_comment = new DataGetSync(question_comment_id, "Zhihu1", "question_comment", 32345, 100);
        t_question_comment.start();
        Thread t_question = new DataGetSync(question_id, "Zhihu1", "question", 32345, 100);
        t_question.start();
        Thread t_comment2 = new DataGetSync(commentid2, "Zhihu1", "comment", 32345, 100);
        t_comment2.start();
        Thread t_answer = new DataGetSync(answer_id, "Zhihu1", "answer", 32345, 100);
        t_answer.start();
        Thread t_user = new DataGetSync(user_id, "Zhihu1", "user", 32345, 100);
        t_user.start();


    }

}
