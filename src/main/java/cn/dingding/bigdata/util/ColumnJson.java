package cn.dingding.bigdata.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * Created by dingwei on 2018/8/1.
 */
public class ColumnJson {

    public static JSONObject convent(CanalEntry.Column column){
        JSONObject js = new JSONObject();
        js.put("name",column.getName());
        js.put("isKey",column.getIsKey());
        js.put("mysqlType",column.getMysqlType());
        js.put("value",column.getValue());
        js.put("isNull",column.getIsNull());
        js.put("index",column.getIndex());
        js.put("updated",column.hasUpdated());
        return js;
    }

}
