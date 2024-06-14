local utility = require("utility")
local args = table.pack(...)
-- first argument is the path to the lua script
local cb_bootstrap_host = args[2] -- or "cb001.example.com"
local cb_bootstrap_port = args[3] -- or 11207
local cb_bucket_name = args[4] -- or "sample-bucket"
local use_ssl = utility.str_to_bool(args[5]) -- or false
local dns_server = args[6] -- or "127.0.0.1"
local dns_port = args[7] -- or 53
local dns_query_type = args[8] -- or "A"

local core = core
local cb_request_queue = core.queue()
local couchbase = require("couchbase_core")
local initialized = false
local cb_result_map = {}
local function run_couchbase_adapter()
  if not initialized then
    print("haproxy_lua_couchbase: cb_bootstrap_host, cb_bootstrap_port, cb_bucket_name, use_ssl, dns_server, dns_port, dns_query_type",
    cb_bootstrap_host, cb_bootstrap_port, cb_bucket_name, use_ssl, dns_server, dns_port, dns_query_type)
    couchbase.init_session(cb_bootstrap_host, cb_bootstrap_port, cb_bucket_name, use_ssl, dns_server, dns_port, dns_query_type)
    core.msleep(100)
  end
  while true do
    local size = cb_request_queue:size()

    if size > 50 then
      size = 50
    end

    local host_request_map = { }
    if size > 0 then
      print("cb_test: size of the queue: ", size)
    end
    for i = 1, size do
      local cb_req = cb_request_queue:pop()
      -- print("cb_test: cb_req: ", utility.dump(cb_req))
      local cb_key, req_uuid, req_enq_time = cb_req[1], cb_req[2], cb_req[3]
      local enqued_since = core.now().usec  - req_enq_time.usec
      -- print("cb_test: enqued_since: ", enqued_since)
      local vBucket, cb_host = couchbase.get_bucket_id_host_for_key(cb_key)
      -- print("cb_test: vBucket, cb_host, cb_key, req_uuid, req_enq_time: ", vBucket, cb_host, cb_key, req_uuid, req_enq_time)
      local cb_cmd = couchbase.encode_request_pack(0, cb_key, vBucket, req_uuid)
      host_request_map[cb_host] = host_request_map[cb_host] or {}
      host_request_map[cb_host][req_uuid] = cb_cmd
    end
    
    for host, uuid_cmds_map in pairs(host_request_map) do
      local cb_client = couchbase.get_cb_client(host)
      local cb_results, err  = couchbase.run_batch_get_key(cb_client, uuid_cmds_map)
      -- print("cb_test: cb_results: ", utility.dump(cb_results))
      for cb_key, cb_value in pairs(cb_results) do
        -- print("cb_test: cb_key, cb_value: ", cb_key, cb_value)
        cb_result_map[cb_key] = {cb_value, err}
        -- print("cb_test: after setting the cb key value: cb_result_map: ", utility.dump(cb_result_map))
      end
      if err then
        print("cb_test: Error in batch get: ", err)
        couchbase.init_session(host, cb_bootstrap_port, cb_bucket_name)
      end      
    end

  core.msleep(1)
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
    print("haproxy_lua_couchbase: cb_client is nil, trying on emore time")
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
    -- print("haproxy_lua_couchbase: response_data, response_uuid, err: ", response_data, " :",  response_uuid, " :", err)
    if err then
      print("haproxy_lua_couchbase: error in teh connection:", err)
      txn:set_var("txn.cbvalue", "")
      print("resetting cb client")
      couchbase.reset_cb_client(cb_host, cb_client)
      return
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
        txn:set_var("txn.cbvalue", "")
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
