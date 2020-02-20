package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class Test2 extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {


        List<String> fieldNames=new ArrayList<>();
        List<ObjectInspector> fieldOis=new ArrayList<>();
        //添加类型名字
        fieldNames.add("event_name");
        //添加类型
        fieldOis.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_start");
        fieldOis.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOis);
    }


    @Override
    public void process(Object[] objects) throws HiveException {
            //因为塞入的可以为数组我们去第一个就参数就行了
        //获取et
        String input = objects[0].toString();

        try {
            //判断et里的数据的是否为空
            if(StringUtils.isBlank(input)){
                return ;
            }else{
                JSONArray ja = new JSONArray(input);
                //因为有UDTF是一进多出所以可以理解为自定义行转列
                //所以我们给他规定了几列可做
                //遍历数组因为数组可能有多个

                for (int i = 0; i < ja.length(); i++) {
                    String[] result = new String[2];
                    try {
                        //获取事件名event_name
                        result[0]= ja.getJSONObject(i).getString("en");
                        //获取事件的整个json
                        result[1]= ja.getString(i);
                    }catch (JSONException e){
                        continue;
                    }
                    forward(result);
                }
            }
            
            
           
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
