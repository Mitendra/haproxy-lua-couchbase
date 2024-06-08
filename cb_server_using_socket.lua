local socket = require('socket')
local server = assert(socket.bind("*", 11207))
local tcp = assert(socket.tcp())

print(socket._VERSION)
print(tcp)


local utility = require("utility")
local CB_CMD_GET =  0x00 -- => 31
local CB_CMD_SELECT_BUCKET = 0x89 -- => 137
local CB_CMD_HELLO = 0x1f -- ==>
local CB_CMD_GET_CLUSTER_CONFIG = 0xb5 -- => 181>
local function read_cluster_confog_file()
  local file = io.open("mock_cluster_config.json", "r")
  local content = file:read("*a")

  file:close()
  return content
end

local cluster_config_data = read_cluster_confog_file()

local function handle_request(applet)
  local index = 0
  local data, bytes_sent, err
  while true do

    data, err  = applet:receive(24)
    if err then
      print("cb server: error while receiving data: ", err)
      return 0
    end
    --utility.printByteArray(utility.stringToByteArray(data))
    -- check the command type
    local command_code = string.unpack("B", data, 2)
   -- print("cb command_code", command_code)
    if command_code == 31 then
      print("cb Hello command received")
      response = "\129\31\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
      print("sending response")
      applet:send(response)
    elseif command_code == 137 then
      --print("cb select bucket command received")
      local body_length = string.unpack(">H", data, 3)
      --print("cb body length:", body_length)
      local data, err = applet:receive(body_length)
      if err then
        print("cb server: error while receiving data: ", err)
        return 0
      end
      print("cb received bucket comand for: ", utility.get_byte_seq_as_str(data))
      response = "\129\137\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
      print("sending response")
      bytes_sent, err =  applet:send(response)
    elseif command_code == 181 then
      print("cb get config command received")
      --response_header = "\129\181\0\0\0\0\0\0\0\0\43\59\0\0\0\0\0\0\0\0\0\0\0\0"
      response_header = "\129\181\0\0\0\0\0\0\0\0\43\60\0\0\0\0\0\0\0\0\0\0\0\0"
      print("sending response")
      applet:send(response_header)
      --print("sending config data of size", #cluster_config_data)
      applet:send(cluster_config_data)
    elseif command_code == 0 then
      local body_length = string.unpack(">H", data, 3)
      --print("cb body length:", body_length)
      local data = applet:receive(body_length)

      --print("cb get key command received for:", utility.get_byte_seq_as_str(data))
      response_header = "\129\0\0\0\4\0\0\0\0\0\0\35\0\0\4\210\22\156\203\164\167\175\0\0"
      --print("sending response")
      applet:send(response_header)
      --cb_value = "\0\0\0\0\172\237\0\5\116\0\24\73\111\69\75\56\57\108\104\52\75\101\67\67\108\43\86\118\65\85\111\103\65\61\61"
      cb_value = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
      bytes_sent, err = applet:send(cb_value)
      if err then
        print("cb server: error while receiving data: ", err)
        return 0
      end

      print("index: ", index)
      if (index % 1000) == 999 then
     --   core.msleep(50)
      end
      index = index + 1
      --print("index after ", index)
    else
      print("unknown command")
    end
  end -- while end

end

while 1 do
  print("cb sever: starting to accept connection")
  local cb_server = server:accept()
  cb_server:settimeout(20)
  handle_request(cb_server)
  print("cb server: connection closed")
end


