-- randblock.lua — hit random blocks within neve's stored range.
-- Set lo/hi from /health: blocks.min_height .. blocks.max_contiguous_height.
-- A non-zero "Non-2xx" line from wrk means the range is wrong (out-of-range
-- heights return HTTP 421); fix lo/hi and rerun.
--
-- For a node-vs-neve comparison, set lo/hi to the OVERLAP of neve's range and
-- the node's usable range so both targets serve identical work, and use the
-- matching randblock-node.lua against avalanchego.
math.randomseed(os.time())
local lo, hi = 86703873, 86881651   -- overlap range as of 2026-05-31; refresh each session
request = function()
  local h = math.random(lo, hi)
  local body = string.format(
    '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x%x",false]}', h)
  return wrk.format("POST", "/", {["Content-Type"] = "application/json"}, body)
end
