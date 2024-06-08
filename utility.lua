_M = {}
_M.stringToByteArray = function(s)
    local byteArray = {}
    for i = 1, #s do
        byteArray[i] = string.byte(s, i)
    end
    return byteArray
end

_M.printByteArray = function(byteArray)
  print(table.concat(byteArray, ","))
end

local polynomial = 0xEDB88320
_M.crc32 = function(str)
    local crc = 0xFFFFFFFF
    for i = 1, #str do
        local byte = string.byte(str, i)
        crc = crc ~ byte
        for _ = 1, 8 do
            if crc % 2 == 1 then
                crc = (crc >> 1) ~ polynomial
            else
                crc = crc >> 1
            end
        end
    end
    return crc ~ 0xFFFFFFFF
end


_M.dump =  function(o, indent)
    indent = indent or ""  -- Default indent is an empty string
    local nextIndent = indent .. "  "  -- Increase the indent for nested tables

    if type(o) == "table" then
        local s = "{\n"
        for k, v in pairs(o) do
            if type(k) ~= "number" then k = '"' .. k .. '"' end
            s = s .. indent .. "  [" .. k .. "] = " .. dump(v, nextIndent) .. ",\n"
        end
        return s .. indent .. "}"
    else
        return tostring(o)
    end
end

_M.get_byte_seq_as_str =  function(byteSequence)
  local str = ""
  for i = 1, #byteSequence do
    str = str .. string.char(byteSequence:byte(i))
  end
  return str

end 

local function _get_env_var(var_name, default_value)
--  print("utility: var_name, default_value", var_name, default_value)
  local str = os.getenv(var_name)
  if not str then
    return default_value
  else
    return str
  end
end

local function _str_to_bool(str)
  if not str then
    return false
  elseif str ~= "true" and  str ~= "True" then
    return false
  else
    return true
  end
end

_M.get_env_var_as_bool = function(var_name, default_value)
  local result = _get_env_var(var_name, default_value)
  return _str_to_bool(result)  
end
_M.get_env_var = _get_env_var
_M.str_to_bool = _str_to_bool
return _M


