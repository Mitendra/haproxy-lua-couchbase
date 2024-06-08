local resolver = require "dns_resolver"
local r
r = resolver.dns_query("google.com", "A", "8.8.8.8", 53)
print("result: ", r)
r = resolver.dns_query("google.com", "AAAA", "8.8.8.8", 53)
print("result: ", r)

