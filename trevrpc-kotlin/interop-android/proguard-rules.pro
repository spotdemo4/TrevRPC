# The app has no code of its own for R8 to start from, so it keeps the generated interop clients and
# TrevRPC's public API, which an app calls, and R8 traces core and transport-cronet from them.
-keep class trevrpc.interop.v1.** { *; }
-keep public class zip.trev.trevrpc.** { public *; }
