# zstd-jni is optional: apps add it only when they configure compression, and Compression.kt reports
# its absence at run time.
-dontwarn com.github.luben.zstd.**

# zstd-jni ships no shrinker rules, and its native code finds its classes, fields and constructors by
# name, such as the nativePtr of the dictionaries. The zstd-jni classes an app uses keep their names
# and members; the ones it doesn't use are still removed.
-keepnames class com.github.luben.zstd.**
-keepclassmembers class com.github.luben.zstd.** { *; }
