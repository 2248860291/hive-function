package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

//缁ф壙UDF绫�
public class BaseFieldUDF extends UDF {
    //鍒涘缓涓�涓粯璁ょ殑鏂规硶 evaluate
    public String evaluate(String line,String jsonKeysString){
        //鍑嗗涓�涓� sb String鐨勫垵濮嬪寲鍔犺浇
        StringBuilder sb = new StringBuilder();
        //鍏叡瀛楁閲岀殑key鍚嶇О //1577438761314|{"cm":{"ln":"-115.5","sv":"V2.1.4","os":"8.0.3",//        // "g":"XKE645JK@gmail.com","mid":"m226","nw":"3G","l":"es","vc":"15",//        // "hw":"640*960","ar":"MX","uid":"u766","t":"1577427310727","la":"25.1",//        // "md":"Huawei-13",//        // "vn":"1.1.8","ba":"Huawei","sr":"Y"},"ap":"gmall","et":[]}//        //1 鍒囧壊jsonKeys mid uid vc vn l sr
        String[] jsonkeys  = jsonKeysString.split(",");
        //2 澶勭悊 line 鏈嶅姟鍣ㄦ椂闂� | json
        String[] logContents = line.split("\\|");
        //logContents[1])鑾峰彇鎵嬫満鍙�++66
        //1577438761314
        // 3 鍚堟硶鎬ф牎楠�
        if(logContents.length!=2  || StringUtils.isBlank(logContents[1])){
                return "";
        }
        //4 寮�濮嬪鐞唈sone
        //鑾峰彇姣忎釜鏃堕棿鎴抽噷鐩稿簲鐨刯son
        //杩欑殑绗竴涓叕鍏卞瓧娈礳m
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);
            //鑾峰彇cm閲岄潰鐨勫璞�--鍏叡瀛楁
            JSONObject base = jsonObject.getJSONObject("cm");
            //寰幆閬嶅巻鍙栧��
            for (int i = 0; i < jsonkeys.length; i++) {
                //trim鍚у乏鍙�2杈圭┖鏍煎幓鎺�
                String filedName  = jsonkeys[i].trim();
                if(base.has(filedName)){
                    sb.append(base.getString(filedName)).append("\t");
                }else{
                    sb.append("").append("\t");
                }
            }
            //绗簩涓簨浠跺瓧娈�
            sb.append(jsonObject.getString("et")).append("\t");
            //logContents[1])鑾峰彇鎵嬫満鍙�
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String line="1577438761349|{\"cm\":{\"ln\":\"-103.0\",\"sv\":\"V2.4.5\",\"os\":\"8.1.7\",\"g\":\"F2ATBJ2Y@gmail.com\",\"mid\":\"m303\",\"nw\":\"3G\",\"l\":\"en\",\"vc\":\"18\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"u698\",\"t\":\"1577423773934\",\"la\":\"-19.3\",\"md\":\"HTC-16\",\"vn\":\"1.3.1\",\"ba\":\"HTC\",\"sr\":\"O\"},\"ap\":\"gmall\",\"et\":[{\"ett\":\"1577405510390\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"3\",\"newsid\":\"n908\",\"news_staytime\":\"4\",\"loading_time\":\"2\",\"action\":\"2\",\"showtype\":\"1\",\"category\":\"11\",\"type1\":\"325\"}},{\"ett\":\"1577425502757\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"5\",\"action\":\"4\",\"detail\":\"\",\"source\":\"4\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"2\"}},{\"ett\":\"1577358990035\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1577400013988\",\"action\":\"4\",\"type\":\"1\",\"content\":\"\"}},{\"ett\":\"1577355739940\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"1\",\"push_id\":\"2\"}},{\"ett\":\"1577400040944\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}},{\"ett\":\"1577342535000\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":4,\"addtime\":\"1577403806068\",\"praise_count\":851,\"other_id\":2,\"comment_id\":0,\"reply_count\":23,\"userid\":2,\"content\":\"婀挎悙鏆栫槦鏈藉爼浜獑\"}},{\"ett\":\"1577421116354\",\"en\":\"favorites\",\"kv\":{\"course_id\":1,\"id\":0,\"add_time\":\"1577410625816\",\"userid\":5}},{\"ett\":\"1577420044385\",\"en\":\"praise\",\"kv\":{\"target_id\":9,\"id\":2,\"type\":1,\"add_time\":\"1577403046563\",\"userid\":8}}]}";
        String x = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }

}
