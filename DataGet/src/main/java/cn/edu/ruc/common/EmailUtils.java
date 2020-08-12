package cn.edu.ruc.common;

import org.apache.commons.mail.SimpleEmail;
import org.apache.log4j.Logger;
import java.util.Date;

/**
 * 通用邮件处理类
 * @author huangzhen
 */
public class EmailUtils {

    private static final Logger log = Logger.getLogger(EmailUtils.class);

    public static void sendEmail(String message){
        SimpleEmail email = new SimpleEmail();
        email.setHostName("smtp.sina.com");//设置使用发电子邮件的邮件服务器
        try {
            email.addTo("xituosoul@sina.com");
            email.setAuthentication("xituosoul@sina.com","196412sina.,");
            email.setFrom("xituosoul@sina.com");
            email.setSubject("微博数据获取异常提醒");
            email.setMsg(message + new Date());
            email.setCharset("UTF-8");
            email.send();
        }
        catch (Exception ex) {
            log.error("Send email error : " + message , ex);
        }

    }
}
