package com.example.raftkv.util.json;


import com.fasterxml.jackson.databind.ObjectMapper;


/** Single shared ObjectMapper for JSON (thread-safe). */
public final class Json {
public static final ObjectMapper M = new ObjectMapper();
}