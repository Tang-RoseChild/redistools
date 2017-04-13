if ((#KEYS == 0) or (#ARGV == 0)) then
  return
end
local union_result_key = ARGV[1]
local field_values = {}

for i = 1, #KEYS do
    local result = redis.call('hgetall', KEYS[i])
    for i = 1,#result,2 do
        if(not field_values[result[i]]) then
            field_values[result[i]] = 0
        end
        field_values[result[i]] = field_values[result[i]] + result[i+1]
    end
end
for k, v in pairs(field_values) do
    redis.call('zadd', union_result_key, v,k)
end