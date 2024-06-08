# haproxy-lua-couchbase
couchbase integration in haproxy using Lua
# Usage
1. Load the Lua script in the global section of your haproxy.cfg
   ```
   lua-prepend-path "./path-to-haproxy-lua-couchbase-dir/?.lua" # point to the path of haproxy-lua-couchbase 

   # param1 : bootstrap host
   # param2 : bootstrap port
   # param3 : couchbase bucket name 
   # param4 : use ssl or not
   # param5 : dns server name
   # param6 : dns port
   # param7 : dns record type
   lua-load-per-thread haproxy_lua_couchbase.lua cb001.example.com 11207 sample-bucket false 127.0.0.1 8053 A
   ```
2. In your frontend/backend set the key for which the couchbase lookup needs to be done in the txn.cbkey variable
    ```
    http-request set-var(txn.cbkey) urlp(key)
    ```
3. Call the getCBKey action to get the value
   http-request lua.getCBKey 
4. The result will be set in txn.cbvalue variable

# Parameters
1. param1 : bootstrap host
2. param2 : bootstrap port
3. param3 : couchbase bucket name 
4. param4 : use ssl or not
5. param5 : dns server name
6. param6 : dns port
7. param7 : dns record type

# Inner Workings

# couchbase mock server
This repository also has a Mock cb_server for testing purpose

## Usage
1. Load the lua script in the global section
   ```
   lua-load-per-thread cb_server.lua
   ```
2. Add a frontend with the below details
   ```
    frontend cb_server
        bind *:11207 #crt ~/identity.pem  ca-file ./ca-bundle.crt  ca-verify-file ./ca-bundle.crt
        mode tcp
        option tcplog
        tcp-request content use-service lua.cb_server
    ```
3. This mock server uses the data from mock_cluster_config.json
4. The cluster config contains hostname as *.example.com. to make it work you may need to update the /etc/host to point all the hosts to 127.0.01. Add the following lines to your /etc/hosts
   ```
   127.0.0.1 cb001.example.com
   127.0.0.1 cb002.example.com
   127.0.0.1 cb003.example.com
   127.0.0.1 cb004.example.com
   127.0.0.1 cb005.example.com
   ```
5. you can also use dnsmasq for dns updates. 
   1. For that, update the dnsmasq setting :
   ```
   address=/example.com/127.0.0.1
   ```
   2. 

# HAProxy lua socket patch for mtls
Current version of haproxy 3.0 does not support mtls in the lua tcp. the followng patch enables it. This default the crt to be used as identity.cert and ca-file to be used as ca-bundle.crt. So you may need to cpy these files with these specific names or update the patch to point to the right files
```
diff --git a/src/hlua.c b/src/hlua.c
index 098107f7a..3a86dcf1d 100644
--- a/src/hlua.c
+++ b/src/hlua.c
@@ -14065,6 +14065,10 @@ void hlua_init(void) {
    "ssl",
    "verify",
    "none",
+   "crt",
+   "identity.cert",
+   "ca-file",
+   "ca-bundle.crt",
    NULL
  };
 #endif
~
~
~
~
~
```
