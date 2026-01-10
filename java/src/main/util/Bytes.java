package com.example.raftkv.util;


import java.nio.charset.StandardCharsets;


/** Small helpers for UTF-8 conversions. */
public final class Bytes {
public static byte[] utf8(String s) {
return s.getBytes(StandardCharsets.UTF_8);
}


public static String str(byte[] b) {
return new String(b, StandardCharsets.UTF_8);
}
}