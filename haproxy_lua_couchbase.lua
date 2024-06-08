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
    local cb_result = nil
    -- print("cb_test: get_cb_key: ", key, uuid)
    cb_request_queue:push({key, uuid, core.now()})
    local i = 1
    while i < 6 do
      core.msleep(1)
      -- cb_result = cb_result_map[key]
      -- print("cb_test:  cb_result_map: ",utility.dump(cb_result_map))
      cb_result = cb_result_map[uuid]
      if cb_result  then
        break
      end 
      i = i + 1
    end
    print("cb_test: found after n ms: ", i)
    if cb_result and cb_result[1] then
      txn:set_var("txn.cbvalue", cb_result[1])
    else
      print("cb_test: cb_result is empty")
      txn:set_var("txn.cbvalue", "")
    end
    cb_result_map[key] = nil
end

core.register_task(run_couchbase_adapter)
core.register_action("getCBKey", {"http-req"}, get_cb_key, 0)
