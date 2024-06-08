local utility = require("utility")
local get_env_var = utility.get_env_var
local get_env_var_as_bool = utility.get_env_var_as_bool
local use_socket = get_env_var_as_bool("use_socket", false)
_M = {}
-- Function to convert a domain name to DNS query format
local function _encode_domain(domain)
    local parts = {}
    for part in domain:gmatch("[^.]+") do
        table.insert(parts, string.char(#part) .. part)
    end
    return table.concat(parts) .. "\0"
end

-- Function to build a DNS query
local function _build_query(domain, query_type)
    local transaction_id = "\1\0"  -- 16-bit identifier
    local flags = "\1\0"           -- Standard query
    local questions = "\0\1"       -- 1 question
    local answer_rrs = "\0\0"      -- 0 answers
    local authority_rrs = "\0\0"   -- 0 authority records
    local additional_rrs = "\0\0"  -- 0 additional records
    local query = _encode_domain(domain)
    local qtype
    --print("query type : ", query_type)
    if query_type == "A" then
      qtype = "\0\1"           -- Type A
    else
      qtype = "\0\28"  -- Type AAAA
    end
    local qclass = "\0\1"          -- Class IN

    return transaction_id .. flags .. questions .. answer_rrs .. authority_rrs .. additional_rrs .. query .. qtype .. qclass
end

-- Function to parse the DNS response (simple example for A records)
local function _parse_response(response)
    --print("Raw response (hex):")
    for i = 1, #response do
    --   io.write(string.format("%02x ", response:byte(i)))
    end
   -- print("\n")
    local transaction_id = response:sub(1, 2)
    local flags = response:sub(3, 4)
    local qdcount = response:sub(5, 6)
    local ancount = response:sub(7, 8)
    local nscount = response:sub(9, 10)
    local arcount = response:sub(11, 12)

    -- Skipping the question section (simplified, assuming only one question)
    local pos = 13
    while response:byte(pos) ~= 0 do
        pos = pos + 1
    end
    pos = pos + 5
    --print("pos: ", pos)
    -- Reading the answer section
    if ancount ~= "\0\0" then
      for i = 1, (ancount:byte(1) * 256 + ancount:byte(2)) do
        local name = response:sub(pos, pos + 1)
        local atype = response:sub(pos + 2, pos + 3)
        local aclass = response:sub(pos + 4, pos + 5)
        local ttl = response:sub(pos + 6, pos + 9)
--        local rdlength = response:sub(pos + 10, pos + 11)
        local rdlength = string.unpack(">H", response, pos + 10)
        --print("rdlength: ", rdlength)
--        local rdata = response:sub(pos + 12, pos + 15)
        --local rdata = response:sub(pos + 12, pos + 11 + (rdlength:byte(1) * 256 + rdlength:byte(2)))
        local rdata = response:sub(pos + 12, pos + 11 + rdlength)

        --print("rdata length: ", #rdata)
        if atype == "\0\28" then  -- Type AAAA
            local ip = {}
            for j = 1, 16, 2 do
              table.insert(ip, string.format("%x%02x", rdata:byte(j), rdata:byte(j+1)))
            end
            local ip_final = table.concat(ip, ":")   
            --print("Answer: ", ip_final)
            return ip_final
        elseif atype == "\0\1" then  -- Type A
          local ip = {}
          for j = 1, 4 do
              table.insert(ip, rdata:byte(j))
          end
          local ip_final = table.concat(ip, ".")
          --print("IPv4 Address: ", ip_final)
          return ip_final
        else
          print("Unexpected record type:", atype)
        end
          pos = pos + 12 + rdlength --(rdlength:byte(1) * 256 + rdlength:byte(2))
      end
    else
        print("No answers received.")
        return nil
    end
end

-- Main function to perform the DNS query
local function _dns_query(domain, qtype, nsname, nsport)
    print("dns_resolver: nsname, nsport", nsname, nsport)
    local tcp
    if use_socket then
      socket = require("socket")
      tcp =  assert(socket.tcp())
    else
      tcp = core.tcp()
    end
    tcp:connect(nsname, nsport)

    local query = _build_query(domain, qtype)
    local length = string.char(#query // 256, #query % 256)

    tcp:send(length .. query)

    local response_length = tcp:receive(2)
    local length = (response_length:byte(1) * 256) + response_length:byte(2)
    local response = tcp:receive(length)

    local ip = _parse_response(response)
    tcp:close()
    return ip
end
_M.dns_query = _dns_query
return _M
-- Example usage
--local domain = "google.com"
--ip = dns_query(domain, "A")
--ip = dns_query(domain, "AAAA")
--print("ip ", ip)
