package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

public class EventJsonUDTF extends GenericUDTF {

    // 该方法中，我们将指定输出参数的名称和参数类型
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOis){
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs  = new ArrayList<>();
        //参数名称
        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        //参数类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //参数的名称和参数类型
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }
    
    @Override
    public void process(Object[] objects) throws HiveException {
        //获取传入的et
        String input = objects[0].toString();
        //通过StringUtils工具类的isblank方法判断input是空的
        if(StringUtils.isBlank(input)){
            //返回无
            return;
        }//不为空返回结果
        else{
            //et是数组所以使用JSONArray
            try {
                JSONArray ja = new JSONArray(input);
                //如果ja是空的则返回无
                if(ja==null){
                    return;
                }
                //循环遍历每一个事件
                //json串解析成的
                for (int i = 0; i < ja.length(); i++) {
                    String[] result  = new String[2];
                    try {
                        //取出没个事件的名称
                        result [0] = ja.getJSONObject(i).getString("en");
                        //取出每一个事件整体
                            result[1]=ja.getString(i);
                        }catch (JSONException e){
                        continue;
                    }
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }


        }

    }

    @Override
    public void close() throws HiveException {

    }
}
