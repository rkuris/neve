-- randblock-node.lua — same as randblock.lua, but pointed at an avalanchego
-- C-chain RPC endpoint (POST /ext/bc/C/rpc on port 9650) instead of neve.
-- Use the SAME lo/hi as randblock.lua (the overlap of neve's range and the
-- node's usable range) so both targets serve identical work.
--
-- Unlike neve, an out-of-range height here does NOT return HTTP 421 — the node
-- answers with a JSON null result (still HTTP 200), so wrk won't flag it. Keep
-- lo/hi inside the node's usable range or you'll silently benchmark null reads.
math.randomseed(os.time())
local lo, hi = 86703873, 86881651   -- overlap range as of 2026-05-31; refresh each session
request = function()
  local h = math.random(lo, hi)
  local body = string.format(
    '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x%x",false]}', h)
  return wrk.format("POST", "/ext/bc/C/rpc", {["Content-Type"] = "application/json"}, body)
end
