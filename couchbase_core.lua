local utility = require("utility")
local get_env_var = utility.get_env_var
local get_env_var_as_bool = utility.get_env_var_as_bool
local use_socket = get_env_var_as_bool("use_socket", false)
print("couchbase_core: use_socket: ", use_socket)
-- local use_mock = get_env_var_as_bool("use_mock", true)
-- print("couchbase_core: use_mock: ", use_mock)
local core = core
local printByteArray = utility.printByteArray
local stringToByteArray = utility.stringToByteArray
local socket 
local resolver 
if use_socket then
  socket = require("socket")
else
  resolver = require("dns_resolver") 
end
local json = require ("dkjson")
local _M = {}
_M.shared_data = {
  host_ip_map = {},
  host_client_map = {},
  port = "11207",
  bucket_name = "",
  use_ssl = false,
  dns_port = 53,
  dns_server = "127.0.0.1",
  dns_query_type = "A",

  server_list = {},
  buckets_map = {},
  init_done = false,
  last_config_update_time  = core.now().sec

}

local CB_CMD_GET =  0x00
local CB_CMD_SELECT_BUCKET = 0x89
local CB_CMD_HELLO = 0x1f
local CB_CMD_GET_CLUSTER_CONFIG = 0xb5

local function _get_tcp_connection_using_socket(host_ip, port)
  local ssl = require("ssl")
  local params = {
   mode = "client",
   protocol = "tlsv1_3",
   key = "./identity.key",
   certificate = "identity.cert",
   cafile = "./ca-bundle.crt",
   --verify = {"peer", "fail_if_no_peer_cert"},
   verify = "peer",
   options = {"all", "no_sslv2"},
  }

  cb_client = socket.tcp()
  local r = cb_client:settimeout(2,r)
  r = cb_client:settimeout(2,w)
  r = cb_client:settimeout(200,t)
  cb_client:connect(host_ip, port)
  cb_client = assert( ssl.wrap(cb_client, params) )
  assert( cb_client:dohandshake() )
  return cb_client 
end

-- local DNS_RECORD_TYPE = get_env_var("CB_RECORD_TYPE", "A")
-- local DNS_PORT = tonumber(get_env_var("DNS_PORT", 53))
-- local DNS_SERVER = get_env_var("DNS_SERVER", "127.0.0.1")
--print("coucbase_core: DNS_RECORD_TYPE, DNS_SERVER, DNS_PORT: ", DNS_RECORD_TYPE, DNS_SERVER, DNS_PORT)
local function _get_host_ip(hostname)
  local host_ip = _M.shared_data.host_ip_map[hostname]
  if not host_ip then
    if use_socket then
      host_ip = socket.dns.getaddrinfo(hostname)[1]["addr"]
    else
      --host_ip = resolver.dns_query(hostname, "AAAA", "172.29.152.22", 53)
      host_ip = resolver.dns_query(hostname,_M.shared_data.dns_query_type, _M.shared_data.dns_server, _M.shared_data.dns_port)
    end
    _M.shared_data.host_ip_map[hostname] = host_ip
  end
  return host_ip
end

local function _get_tcp_connection_using_haproxy(host_ip, port)
  print("couchbase_core: host_ip, port: ", host_ip, port)
  local cb_client = core.tcp()
  -- local success, err = cb_client:setoption("keepalive", true)
  -- if not success then
  --  print("couchbase_core: Failed to set keep-alive: ", err, "\n")
  -- else
  --   print("couchbase_core: TCP keep-alive enabled")
  -- end
  local r = cb_client:settimeout(5)
  if _M.shared_data.use_ssl then
    print("couchbase_core: using ssl")
    r = cb_client:connect_ssl(host_ip, port)
  else
    print("couchbase_core: using plain text")
    r = cb_client:connect(host_ip, port)
  end

  r = cb_client:settimeout(10)
  return cb_client


end

local _get_bucket_id = function(key)
  local hash = utility.crc32(key)
  local vbucket_id = ((hash >> 16) & 0x7fff) & 1023
  return vbucket_id
end
local _get_cb_host = function(vbucket_id)
  local bucket_server_list = _M.shared_data.buckets_map[vbucket_id + 1] -- indexes are based on 1 in lua
  --print("couchbase_core: bucket_server_list", utility.dump(bucket_server_list))
  local primary_server_index = bucket_server_list[1] -- first one is primary server
  local hostname = _M.shared_data.server_list[primary_server_index + 1] -- server index are 0 based but lua indexes are 1 based
  return hostname
end

local _get_bucket_id_host_for_key = function(key)
  local bucket_id = _get_bucket_id(key)
  --local host, host_ip = _get_cb_host(bucket_id)
  local host = _get_cb_host(bucket_id)
  --print("couchbase_core: key, bucket id, host: ", key, bucket_id, host)
  return bucket_id, host --, host_ip
end

local _encode_request_pack = function(opCode, key, vBucketId, uuid)
  local magicCode = 128
  local opCode = opCode or 0
  local keyLength = #key
  local extrasLength = 0
  local dataType = 0
  local bucket = vBucketId or 0

  local _value = ''
  local bodyLength = #key + #_value
 -- print("couchbase_core: key, _value, bodyLength: ", key,": ",  _value,": ",  bodyLength)
  local _opaque = uuid or 0
  local _cas = 0 
--  print("couchbase_core: _opaque", _opaque)
  return string.pack(">BBHBBHI4I4I8c" .. bodyLength, magicCode, opCode, keyLength,
    extrasLength, dataType, bucket, bodyLength, _opaque, _cas, key, _value)
------------------------------------------------------------------------------------------------------------------------------------------------------
--|  1  |  2  |  3  |   4  |  5  |  6  |  7  |  8  |  9  |  10  |  11  |  12  |  13  |  14  |  15  |  16  |  17  |  18  | 19 | 20 | 21 | 22 | 23 | 24 |
--V     V     V            V     V     V           V                          V                           V                                           V
-----------------------------------------------------------------------------------------------------------------------------------------------------
--^ 1B  ^ 1B  ^    2B      ^ 1B  ^ 1B  ^    2B     ^         4B               ^       4B                  ^         8B                                ^
--|Magic|opCod|  keyLen    |extL |dtype|  vBucket  |       bodylen            |      opaque               |        CAS                                |
------------------------------------------------------------------------------------------------------------------------------------------------------
end

local get_cb_client = function(hostname, port)
  print("couchbase_core: hostname, port: ", hostname, port)
  local host_ip = _get_host_ip(hostname)
  local cb_client
  if use_socket then 
    cb_client = _get_tcp_connection_using_socket(host_ip, port)
  else
    cb_client = _get_tcp_connection_using_haproxy(host_ip, port)
  end

  --print("couchbase_core: host, port", host, port)
  if _M.shared_data.host_client_map[hostname] then
--    print("couchbase_core: closing existing connection")
    local r = _M.shared_data.host_client_map[hostname]:close()
--    print("couchbase_core: closing connection: ", r)
  end  
  _M.shared_data.host_client_map[hostname] = cb_client
--  print("couchbase_core: new connection estabished")
  if not cb_client then
    print("couchbase_core: client is nil")
  end
  return cb_client
end

local _send_get_key_cmd = function(cb_client, command)
  -- print("couchbase_core: prinitng the request:")
  -- printByteArray(utility.stringToByteArray(command))
  local _, err = cb_client:send(command)
  if err then
    print("couchbase_core: err: ", err)
    return err
  else
    -- print("couchbase_core: send: successful")
    return nil
  end
  return nil
end


local _recieve_get_key_cmd = function(cb_client)
  local response_header, response_data, err
  response_header, err = cb_client:receive(24)  -- Read response header
  if err then
      print("couchbase_core: Error receiving data: " .. err)
      return "", nil, err
  end
  -- print("couchbase_core: prinitng the response header:")
  -- printByteArray(utility.stringToByteArray(response_header))
  if type(response_header) ~= "string" then
     print("couchbase_core: response_header is not string")
     print("couchbase_core: header: ", utility.dump(response_header))
     return "", nil, "not a string"
  end
  local response_length = string.unpack(">I4", response_header, 9)
  -- print("couchbase_core: response length", response_length)

  local response_uuid = string.unpack(">I4", response_header, 13)
  -- print("couchbase_core: **** response uuid: ", response_uuid)
  local responseOpCode = string.unpack("B", response_header, 2)
  if( responseOpCode ~= CB_CMD_GET) then
      print("coouchbase_lua: This response is not for the get request. response code: ", responseOpCode)
      return "", "", "IncorrectCommand"
  end
   
  if response_length == 0 then
    return "", response_uuid, nil
  end
  
   response_data, err = cb_client:receive(response_length)
  if err then
    print("couchbase_core: Error receiving response data: " .. err)
    return response_data, response_uuid, err
  end
  return response_data, response_uuid, nil

end
local _run_batch_get_key = function(cb_client, uuid_command_map)
  -- print("couchbase_core: _run_batch_get_key: uuid_command_map: ", utility.dump(uuid_command_map))
  local result = {}
  local err = nil
  for uuid, command in pairs(uuid_command_map) do
      result[uuid] = "pending"
      err = _send_get_key_cmd(cb_client, command)
      if err then
        -- print("couchbase_core: error while sending get key command: ", err)
        -- return nil, err
        break
      end
  end
  -- print("couchbase_core:  result before receive: ", utility.dump(result))
  for uuid, _ in pairs(uuid_command_map) do
    local response_data, response_uuid
    -- print("couchbase_core: waiting for response for uuid: ", uuid)
    response_data, response_uuid, err = _recieve_get_key_cmd(cb_client)
    if err then
      print("couchbase_core: error while receiving get key command: ", err)
      -- return nil, err
      break
    else
      -- print("couchbase_core: response_data: ", response_data, ", response_uuid: ", response_uuid)
    end
    if response_uuid then
      if result[response_uuid] == "pending" then
        result[response_uuid] = response_data
      else
        print("couchbase_core: unexpected uuid in the response: ", response_uuid)
      end
    end
  end
  -- print("couchbase_core:  result after receive: ", utility.dump(result))
  return result, err
end

local run_cb_command = function(cb_client, command, uuid)
 -- print("couchbase_core: run cb command")
  --print("couchbase_core: request bytes")
  --utility.printByteArray(utility.stringToByteArray(command))
  local bytes_sent, err = cb_client:send(command)
  if err then
    print("couchbase_core: err: ", err)
    return "", nil, err
  end
  --print("couchbase_core: bytes_sent:",bytes_sent)

  local response_header, err = cb_client:receive(24)  -- Read response header
  if err then
      print("couchbase_core: Error receiving data: " .. err)
      return "", nil, err
  end
  --print("couchbase_core: prinitng the response header:")
  --utility.printByteArray(utility.stringToByteArray(response_header))
  if type(response_header) ~= "string" then
     print("couchbase_core: response_header is not string")
     print("couchbase_core: header: ", utility.dump(response_header))
     return "", nil, "not a string"
  end
  local response_length = string.unpack(">I4", response_header, 9)
 -- print("couchbase_core: response length", response_length)

  local response_uuid = string.unpack(">I4", response_header, 13)
  -- print("couchbase_core: **** response uuid: ", uuid)

  if response_uuid ~= uuid then
    print("couchbase_core: wrong uuid, request : ", uuid, ", response: ", response_uuid)
    local responseOpCode = string.unpack("B", response_header, 2)
    if( responseOpCode ~= CB_CMD_GET) then
      print("coouchbase_lua: This response is not for the get request. response code: ", responseOpCode)
    end
  end
   
  if response_length == 0 then
    return "", uuid, nil
  end
  local response_data, err = cb_client:receive(response_length)
  if err then
    print("couchbase_core: Error receiving response data: " .. err)
    return response_data, uuid, err
  end
 -- print("couchbase_core: response bytes")
-- utility.printByteArray(utility.stringToByteArray( response_data))
  return response_data, uuid, nil
end

local extract_hostname_list = function(server_port_list)
  local hostnames = {}
    for _, server_port in ipairs(server_port_list) do
        -- Extract the hostname using string.match
        local hostname = string.match(server_port, "([^:]+)")
        if hostname then
            table.insert(hostnames, hostname)
        end
    end
    return hostnames 
end


local extract_bucket_details = function(config_json)
   --print("couchbase_core: config_json:", config_json)
  -- read local file
  local obj, pos, err = json.decode (config_json, 1, nil)
  if err then
    print("couchbase_core: error reading file", err)
  end
  --print(obj["vBucketServerMap"])
  local serverList = obj["vBucketServerMap"]["serverList"]
  local bucketMap = obj["vBucketServerMap"]["vBucketMap"]
  --print(utility.dump(bucketMap))
  local host_list = extract_hostname_list(serverList)
  return host_list, bucketMap, nil
end


--_M.mt.__index.update_cluster_config = function()
local update_cluster_config = function(cb_client)
    local command = _encode_request_pack(CB_CMD_GET_CLUSTER_CONFIG, '', 0)
  --print("couchbase_core: command:  CB_CMD_GET_CLUSTER_CONFIG")
  --printByteArray(stringToByteArray(command))
  local config, uuid, err = run_cb_command(cb_client, command, 0)
  if err then
    return nil, nil, err
  end
--  print("couchbase_core: config: ", config)
  _M.shared_data.server_list, _M.shared_data.buckets_map, err = extract_bucket_details(config) 
  if err then
    print("couchbase_core: unable to get cluster config")
    return err
  else
--    print("couchbase_core: bucket loaded successfully for thread: ", core.thread)
  end
end

local function _get_last_config_update_since()
  local last_update_since = core.now().sec - _M.shared_data.last_config_update_time
--  print("couchbase_core: last_update_since: ", last_update_since)
  return last_update_since
end

local _init_session = function(hostname, port, bucket_name, use_ssl, dns_server, dns_port, dns_query_type)
  _M.shared_data.port =  port or _M.shared_data.port
  _M.shared_data.bucket_name = bucket_name or _M.shared_data.bucket_name
  _M.shared_data.use_ssl =  use_ssl or _M.shared_data.use_ssl or false
  _M.shared_data.dns_server = dns_server or _M.shared_data.dns_server or "127.0.0.1"
  _M.shared_data.dns_port = tonumber(dns_port) or _M.shared_data.dns_port or 53
  _M.shared_data.dns_query_type = dns_query_type or _M.shared_data.dns_query_type or "A"

  local cb_client = get_cb_client(hostname, port)
  local command = _encode_request_pack(CB_CMD_HELLO, '', 0)
  local config, uuid, err = run_cb_command(cb_client, command, 0)
  if err then
    print("couchbase_core: command hello errored out:", err)
    return nil
  end
--  print("couchbase_core: response for CB_CMD_HELLO: ", config, " : ")
  command = _encode_request_pack(CB_CMD_SELECT_BUCKET, bucket_name, 0)
  --printByteArray(stringToByteArray(command))
  config, uuid, err = run_cb_command(cb_client, command, 0)
  if err then
    print("couchbase_core: CB_CMD_SELECT_BUCKET errored out:", err)
    return nil
  end
--  print("couchbase_core: CB_CMD_SELECT_BUCKET succeeded")

  if not _M.shared_data.init_done  or ( _get_last_config_update_since() > 300) then
    update_cluster_config(cb_client)
    _M.shared_data.init_done = true
    _M.shared_data.last_config_update_time = core.now().sec
  end
  print("couchbase_core: session established for thread: ", core.thread)
  return cb_client

end



local _get_cb_client = function(cb_host)
  local cb_client = _M.shared_data.host_client_map[cb_host]
  if not cb_client then
    print("couchbase_core: cb_client is nil")
    cb_client = _init_session(cb_host, _M.shared_data.port, _M.shared_data.bucket_name)  -- need to update the host_client map
  end
  return cb_client  
end

local _get_cb_key = function(key, req_uuid)
  local bucket_id, cb_host = _get_bucket_id_host_for_key(key)
--  print("couchbase_core: cb_host: ", cb_host)
  local command = _encode_request_pack(CB_CMD_GET, key, bucket_id, req_uuid)
  local cb_client = _get_cb_client(cb_host)
--  print("couchbase_core:command: CB_CMD_GET")
--  printByteArray(stringToByteArray(command))
  local st = core.now()
  local value, uuid, err =  run_cb_command(cb_client, command, req_uuid)
  local et = core.now()
  --print("couchbase_core: time taken for this command: sec: ", et.sec - st.sec, ", ms: ", et.usec - st.usec)
  if err then
--    print("couchbase_core: error while trying to get cb key: ", err)
    local connection_closed = string.find(err, "connection closed", 1, true)
    if connection_closed then
      cb_client = _init_session(cb_host, _M.shared_data.port) 
      value, uuid, err =  run_cb_command(cb_client, command, req_uuid)
    else
      print("couchbase_core: error is not related to connetion closed: ", err)
      cb_client = _init_session(cb_host, _M.shared_data.port)
      value, uuid, err =  run_cb_command(cb_client, command, req_uuid)
      return nil, nil, err
    end
    if err then
      print("couchbase_core: error after reconnection as well:", err)
      return nil, nil, err
    else
--      print("couchbase_core: get key worked after restart")
    end
  end
  return value, uuid, err
end

_M.init_session = _init_session
_M.get_cb_key = _get_cb_key
_M.run_batch_get_key = _run_batch_get_key
_M.get_bucket_id_host_for_key = _get_bucket_id_host_for_key
_M.encode_request_pack = _encode_request_pack
_M.get_cb_client = _get_cb_client
return _M
