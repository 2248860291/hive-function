package com.foo.udf;

import org.apache.avro.data.Json;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class Test1 extends UDF {

    public String evaluate(String line,String JsonkeyString) throws JSONException {
        //这个UDF所需要做的作用是吧cm里面的json给抽取出来并且et里的数据也抽取出来抽取出来（数组的形式）和电话
        String[] conlog = line.split("\\|");//判断总共几条合法性
        String[] jsks = JsonkeyString.split(",");//判断需要抽取出的字段
        //一个不可变的数组
        StringBuilder sb = new StringBuilder();
        if(conlog.length!=2 || conlog==null){
            return null;
        }
        //判断抽取出的字段
        for (int i = 0; i < jsks.length; i++) {
            //获取里面的值
            JSONObject jsonObject = new JSONObject(conlog);//获取整条json的信息了
            //需要判断的长度有

            //目的的第一条吧cm里的数据抽取出来
            JSONObject base = jsonObject.getJSONObject("cm");
            //抽取cm字段
            for (int j = 0; j < jsks.length; j++) {
                //因为要每个都遍历输出一边

                String filedName = jsks.toString().trim();//去除下多余的空值
               //如果整个cmjson族里有这个字段
               if(base.has(filedName)){
                   sb.append(base.getString(filedName)).append("\t");
               }else{
                   sb.append("").append("\t");
               }
            }
            //抽取et字段
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(conlog[0]).append("\t");


        }



        return null;
    }


}
