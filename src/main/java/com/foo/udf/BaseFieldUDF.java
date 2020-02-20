package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

//继承UDF类
public class BaseFieldUDF extends UDF {
    //创建一个默认的方法 evaluate
    public String evaluate(String line,String jsonKeysString){
        //准备一个 sb String的初始化加载
        StringBuilder sb = new StringBuilder();
        //公共字段里的key名称 //1577438761314|{"cm":{"ln":"-115.5","sv":"V2.1.4","os":"8.0.3",//        // "g":"XKE645JK@gmail.com","mid":"m226","nw":"3G","l":"es","vc":"15",//        // "hw":"640*960","ar":"MX","uid":"u766","t":"1577427310727","la":"25.1",//        // "md":"Huawei-13",//        // "vn":"1.1.8","ba":"Huawei","sr":"Y"},"ap":"gmall","et":[]}//        //1 切割jsonKeys mid uid vc vn l sr
        String[] jsonkeys  = jsonKeysString.split(",");
        //2 处理 line 服务器时间 | json
        String[] logContents = line.split("\\|");
        //logContents[1])获取手机号
        //1577438761314
        // 3 合法性校验
        if(logContents.length!=2  || StringUtils.isBlank(logContents[1])){
                return "";
        }
        //4 开始处理json
        //获取每个时间戳里相应的json
        //这的第一个公共字段cm
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);
            //获取cm里面的对象--公共字段
            JSONObject base = jsonObject.getJSONObject("cm");
            //循环遍历取值
            for (int i = 0; i < jsonkeys.length; i++) {
                //trim吧左右2边空格去掉
                String filedName  = jsonkeys[i].trim();
                if(base.has(filedName)){
                    sb.append(base.getString(filedName)).append("\t");
                }else{
                    sb.append("").append("\t");
                }
            }
            //第二个事件字段
            sb.append(jsonObject.getString("et")).append("\t");
            //logContents[1])获取手机号
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String line="1577438761349|{\"cm\":{\"ln\":\"-103.0\",\"sv\":\"V2.4.5\",\"os\":\"8.1.7\",\"g\":\"F2ATBJ2Y@gmail.com\",\"mid\":\"m303\",\"nw\":\"3G\",\"l\":\"en\",\"vc\":\"18\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"u698\",\"t\":\"1577423773934\",\"la\":\"-19.3\",\"md\":\"HTC-16\",\"vn\":\"1.3.1\",\"ba\":\"HTC\",\"sr\":\"O\"},\"ap\":\"gmall\",\"et\":[{\"ett\":\"1577405510390\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"3\",\"newsid\":\"n908\",\"news_staytime\":\"4\",\"loading_time\":\"2\",\"action\":\"2\",\"showtype\":\"1\",\"category\":\"11\",\"type1\":\"325\"}},{\"ett\":\"1577425502757\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"5\",\"action\":\"4\",\"detail\":\"\",\"source\":\"4\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"2\"}},{\"ett\":\"1577358990035\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1577400013988\",\"action\":\"4\",\"type\":\"1\",\"content\":\"\"}},{\"ett\":\"1577355739940\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"1\",\"push_id\":\"2\"}},{\"ett\":\"1577400040944\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}},{\"ett\":\"1577342535000\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":4,\"addtime\":\"1577403806068\",\"praise_count\":851,\"other_id\":2,\"comment_id\":0,\"reply_count\":23,\"userid\":2,\"content\":\"湿搐暖瘟朽堕京骆\"}},{\"ett\":\"1577421116354\",\"en\":\"favorites\",\"kv\":{\"course_id\":1,\"id\":0,\"add_time\":\"1577410625816\",\"userid\":5}},{\"ett\":\"1577420044385\",\"en\":\"praise\",\"kv\":{\"target_id\":9,\"id\":2,\"type\":1,\"add_time\":\"1577403046563\",\"userid\":8}}]}";
        String x = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }

}
