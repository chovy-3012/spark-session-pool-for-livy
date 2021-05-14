package io.vergil.livy.sessionpool;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

public class LivyParamInterpreter {

    public static LivyParam decodeParam(String param) {
        LivyParam livyParam = JSONObject.parseObject(param, LivyParam.class);
        return livyParam;
    }

    public static String encodeParam(LivyParam param) {
        String str = JSONObject.toJSONString(param);
        return str;
    }


    @Data
    public static class LivyParam {
        private String sessionId;
        private String statementId;
    }
}
