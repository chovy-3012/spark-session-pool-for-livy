package io.vergil.livy.sessionpool.utils;

import java.util.UUID;

public class GuidUtils {

    public static String newGuild() {
        String s = UUID.randomUUID().toString();
        String s1 = s.replaceAll("-", "");
        return s1;
    }
}
