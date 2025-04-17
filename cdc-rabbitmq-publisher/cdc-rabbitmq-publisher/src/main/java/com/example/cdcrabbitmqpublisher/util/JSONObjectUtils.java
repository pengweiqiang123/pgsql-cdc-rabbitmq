package com.example.cdcrabbitmqpublisher.util;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * ClassName: JSONObjectUtils
 * Package: com.example.cdcrabbitmqpublisher.util
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 15:39
 * @Version 1.0
 */
public class JSONObjectUtils {
    public static Map<String, Object> JsonToMap(String jsonObj) {
        if (jsonObj == null || jsonObj.isEmpty()) {
            throw new IllegalArgumentException("输入的JSON字符串不能为空");
        }
        Map<String, Object> map = new HashMap<>();
        try {
            JSONObject jsonObject = JSONObject.parseObject(jsonObj);
            for (String key : jsonObject.keySet()) {
                Object value = jsonObject.get(key); // 注意：这里假设了T可以被强制转换为JSON对象的值类型
                map.put(key, value);
            }
        } catch (JSONException e) {
            // 日志记录或其他异常处理方法
            throw new IllegalArgumentException("输入的字符串不是有效的JSON格式", e);
        }
        return map;
    }
}
