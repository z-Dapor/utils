package com.zhp.common;

import org.apache.commons.codec.binary.Base64;

public class EncodeUtil {
    public static String encodeBase64(byte[] b) {
        return Base64.encodeBase64URLSafeString(b);
    }

    public static byte[] decodeBase64(String s) {
        return Base64.decodeBase64(s);
    }
}
