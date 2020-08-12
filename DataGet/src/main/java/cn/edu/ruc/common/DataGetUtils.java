package cn.edu.ruc.common;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.zip.GZIPInputStream;

import cn.edu.ruc.dto.IP;
import cn.edu.ruc.dto.Range;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 数据获取基础类
 * @author huangzhen
 */
public class DataGetUtils {
    private static final Logger log = Logger.getLogger(DataGetUtils.class);

    /**
     * 发送post请求
     * @param urlStr 请求路径
     * @param param 请求参数
     * @param times 本次请求重复路径
     * @return
     */
    public static String sendPost(String urlStr, String param, int times){
        //设定最大重复请求次数为5
        if(times > 5){
            return null;
        }
        try{
            URL url = new URL(urlStr);
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.setRequestProperty("Content-type", "application/x-www-form-urlencoded;charset=UTF-8");
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.connect();
            OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream(), "UTF-8");
            out.write(param);
            out.flush();
            out.close();
            BufferedReader bufr = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
            String line = null;
            String result = "";
            while ((line = bufr.readLine()) != null) {
                result = result + line;
            }
            return result;
        }
        catch(Exception e){
            log.error("Get " + urlStr + "?" + param + " error. Try times " + times, e);
            //请求失败，则休息10ms，再重新请求
            try {
                Integer seconds = 1;
                for(Integer ii=0;ii<times;ii++) {
                    seconds *= 2;
                }
                Thread.sleep(10);
                //Thread.sleep(250 * seconds);
            } catch (InterruptedException e1) {
                log.error("Thread sleep error.", e1);
            }
            return sendPost(urlStr, param, times + 1);
        }
    }

    /**
     * 发送get请求
     * @param urlStr 请求路径
     * @param param 请求参数
     * @param times 本次请求重复次数
     * @return
     */
    public static String sendGet(String urlStr, String param, int times){
        //设定最大重复请求次数为5
        if(times > 5){
            return null;
        }
        try {
            String urlNameString = urlStr + "?" + param;
            URL url = new URL(urlNameString);
            // 打开和URL之间的连接
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            if(urlStr.contains("range")){
                conn.setReadTimeout(30000);
                conn.setConnectTimeout(30000);
            }
            else{
                conn.setReadTimeout(120000);
                conn.setConnectTimeout(120000);
            }
            Map<String, List<String>> map = conn.getHeaderFields();
            BufferedReader bufr = null;
            InputStream is = conn.getInputStream();
            if(map.containsKey("Content-Encoding")){
                if(map.get("Content-Encoding").contains("gzip")){
                    GZIPInputStream gzin = new GZIPInputStream(is);
                    bufr = new BufferedReader(new InputStreamReader(gzin, "utf-8"));
                }
            }
            else{
                bufr = new BufferedReader(new InputStreamReader(is, "utf-8"));
            }
            String line = null;
            StringBuilder result = new StringBuilder();
            while ((line = bufr.readLine()) != null) {
                result.append(line);
            }
            conn.disconnect();
            String jsonStr = result.toString();
            if(jsonStr == null || "".equals(jsonStr)){
                log.warn("Get " + urlStr + "?" + param + " empty result. warn.");
                return sendGet(urlStr, param, times + 1);
            }
            else{
                return result.toString();
            }
        }
        catch(Exception e){
            log.error("Get " + urlStr + "?" + param + " error. Try times " + times + " .");
            if(times>=5) {
                EmailUtils.sendEmail("Get " + urlStr + "?" + param + " error. Try times " + times + " ." + e.getMessage() + "\n"+ e.getStackTrace() );
            }
            //请求失败，则休息10ms，再重新请求
            try {
                Integer seconds = 1;
                /*for(Integer ii=0;ii<times;ii++) {
                    seconds *= 2;
                }*/
                Thread.sleep(10);
            } catch (InterruptedException e1) {
                log.error("Thread sleep error.", e1);
            }
            return sendGet(urlStr, param, times + 1);
        }
    }

    /**
     * 数据范围获取
     * @param url 获取url
     * @param param 获取参数
     * @return 数据返回
     */
    public static Range rangeGet(String url, String param){
        Range range = new Range();
        Boolean isSuccess = false;
        Gson gson = new Gson();
        String result = DataGetUtils.sendGet(url, param, 0);
        try{
            range = gson.fromJson(result, Range.class);
        }
        catch(Exception e){
            log.error("Range illegal. " + url + "?" + param, e);
            return null;
        }
        return range;
    }

    /**
     * 获取取数ip地址
     * @param times 本次请求重复次数
     * @return
     */
    public static String ipGet(Integer times) {
        String ip = sendGet("http://117.50.90.76:12345/weixin/get_ip", "auth_usr=douzc", 0);
        Gson gson = new Gson();
        IP result = new IP();
        try {
            result = gson.fromJson(ip, IP.class);
        }
        catch(Exception e) {
            if(times > 5) {
                log.error("Get ip illegal. " + ip, e);
                EmailUtils.sendEmail("Get ip illegal. " + ip);
            }
            try {
                Thread.sleep(10);
            }
            catch(Exception e1) {
                log.error("Thread sleep error. ", e);
            }
            return ipGet(times+1);
        }
        return result.ip;
    }

    /**
     * 数据定长获取
     * @param url 获取url
     * @param param 获取参数
     * @return
     */
    public static List<Document> getSingle(String url, String param, String colName, String dbName){
        List<Document> docs = new ArrayList<>();
        String result = DataGetUtils.sendGet(url, param, 0);
        if(null==result || "".equals(result)){
            return null;
        }
        Gson gson = new Gson();
        try{
            JsonObject obj = new JsonParser().parse(result).getAsJsonObject();
            if("Zhihu1".equals(dbName)) {
                JsonElement sele = obj.get("status");
                int status = sele.getAsInt();
                if(status==0) {
                    log.warn(url +" , "+ colName + " null doc.");
                    return docs;
                }
            }
            JsonArray eles = obj.getAsJsonArray(colName);

            for(JsonElement ele : eles){
                Document doc = Document.parse(ele.toString());
                Document newDoc = new Document();
                if(doc == null){
                    log.warn(url +" , "+ colName + " null doc.");
                    continue;
                }
                for(String key1 : doc.keySet()) {
                    try {
                        String val1 = doc.getString(key1);
                        newDoc.put(key1, val1);

                    } catch (Exception e) {
                        try {
                            Long val1 = doc.getLong(key1);
                            newDoc.put(key1, val1.toString());
                        }
                        catch(Exception e1) {
                            Integer val2 = doc.getInteger(key1);
                            newDoc.put(key1, val2.toString());
                        }
                    }
                }

                if(newDoc.containsKey("id")){
                    //将id转换成mongo的索引_id
                    String idStr = newDoc.getString("id");
                    Long idLong = new Long(idStr);
                    newDoc.remove("id");
                    newDoc.put("_id", idLong);
                }
                docs.add(newDoc);
            }
        }
        catch(Exception e){
            log.error(colName +" result illegal. " + url + "?" + param, e);
            EmailUtils.sendEmail(colName +" result illegal. " + url + "?" + param + " : " + e.getStackTrace());
            return null;
        }
        return docs;
    }


}