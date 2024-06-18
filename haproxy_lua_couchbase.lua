local utility = require("utility")
local args = table.pack(...)
-- first argument is the path to the lua script
local cb_bootstrap_host = args[1] -- or "cb001.example.com"
local cb_bootstrap_port = args[2] -- or 11207
local cb_bucket_name = args[3] -- or "sample-bucket"
local use_ssl = utility.str_to_bool(args[4]) -- or false
local dns_server = args[5] -- or "127.0.0.1"
local dns_port = args[6] -- or 53
local dns_query_type = args[7] -- or "A"

local core = core
local cb_request_queue = core.queue()
local couchbase = require("couchbase_core")
local initialized = false
local cb_result_map = {}
local function run_couchbase_adapter()
  if not initialized then
    print("haproxy_lua_couchbase: cb_bootstrap_host, cb_bootstrap_port, cb_bucket_name, use_ssl, dns_server, dns_port, dns_query_type")
    print("haproxy_lua_couchbase: ", cb_bootstrap_host, cb_bootstrap_port, cb_bucket_name, use_ssl, dns_server, dns_port, dns_query_type )
    
    couchbase.init_session(cb_bootstrap_host, cb_bootstrap_port, cb_bucket_name, use_ssl, dns_server, dns_port, dns_query_type)
    initialized = true
    core.msleep(100)
  end
end


local function get_cb_key(txn)
  local key = txn:get_var("txn.cbkey")
  local uuid = txn.f:rand(65536)
  -- print("haproxy_lua_couchbase: get_cb_key: key, uuid: ", key, uuid)
  local vBucket, cb_host = couchbase.get_bucket_id_host_for_key(key)
  local cb_cmd = couchbase.encode_request_pack(0, key, vBucket, uuid)
  local cb_client = couchbase.get_cb_client(cb_host)
  if not cb_client then
    print("haproxy_lua_couchbase: cb_client is nil, trying one more time")
    cb_client = couchbase.get_cb_client(cb_host)
    if not cb_client then
      print("haproxy_lua_couchbase: cb_client is nil, returning")
      txn:set_var("txn.cbvalue", "")
      return
    end
  end

  local err = couchbase.send_get_key_cmd(cb_client, cb_cmd)
  if err then
   
    print("cb_test: cb_result is empty")
    txn:set_var("txn.cbvalue", "")
    return
  end

  local i = 1
  while i < 6 do
    cb_client = couchbase.get_cb_client(cb_host)
    local response_data, response_uuid, err  = couchbase.recieve_get_key_cmd(cb_client)
    print("haproxy_lua_couchbase: response_data, response_uuid, err: ", response_data, " :",  response_uuid, " :", err)
    if err then
      print("haproxy_lua_couchbase: error in teh connection:", err)
      txn:set_var("txn.cbvalue", "")
      print("resetting cb client")
      couchbase.reset_cb_client(cb_host, cb_client)
    end
    if response_uuid == uuid then
      if response_data then
        txn:set_var("txn.cbvalue", response_data)
      else
        txn:set_var("txn.cbvalue", "")
      end
      return
    elseif response_uuid then
      cb_result_map[response_uuid] = response_data
      -- may be someone else got the result from the cb. 
      -- lets check the map
      local cb_result = cb_result_map[uuid]
      if cb_result  then
        txn:set_var("txn.cbvalue", cb_result)
        table.remove(cb_result_map, uuid)
        return
      end
    else
      print("cb_test: error in recieve_get_key_cmd: ", err)
    end
    i = i + 1
  end    
end

core.register_task(run_couchbase_adapter)
core.register_action("getCBKey", {"http-req"}, get_cb_key, 0)
