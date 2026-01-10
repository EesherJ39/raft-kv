#include <jni.h>
#include <string.h>
#include <stdlib.h>
#include "storage.h"


// JNI bridge: exposes storage_open/put/get/close to Java.


JNIEXPORT jlong JNICALL
Java_com_example_raftkv_Storage_initNative(JNIEnv* env, jclass clazz, jstring jpath) {
const char* path = (*env)->GetStringUTFChars(env, jpath, NULL);
storage* s = (storage*)calloc(1, sizeof(storage));
int rc = storage_open(s, path);
(*env)->ReleaseStringUTFChars(env, jpath, path);
if (rc != 0) {
free(s);
jclass ex = (*env)->FindClass(env, "java/lang/RuntimeException");
(*env)->ThrowNew(env, ex, "storage_open failed");
return 0;
}
return (jlong)(intptr_t)s;
}


JNIEXPORT void JNICALL
Java_com_example_raftkv_Storage_nativePut(JNIEnv* env, jclass clazz,
jlong handle