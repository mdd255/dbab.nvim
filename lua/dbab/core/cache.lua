local schema = require("dbab.core.schema")
local connection = require("dbab.core.connection")

local M = {}

-- Track loading state
M.is_loading_flag = false

---@return boolean
function M.is_loading()
	return M.is_loading_flag
end

--- Returns cached table names ONLY (never triggers DB query)
---@param url? string
---@return string[]
function M.get_table_names_cached(url)
	local target_url = url or connection.get_active_url()
	if not target_url then
		return {}
	end
	return schema.get_cached_table_names(target_url)
end

--- Returns cached columns ONLY (never triggers DB query)
---@param url? string
---@return Dbab.Column[]
function M.get_all_columns_cached(url)
	local target_url = url or connection.get_active_url()
	if not target_url then
		return {}
	end
	return schema.get_cached_columns(target_url)
end

--- Pre-warm cache by loading all tables and columns asynchronously
---@param callback? fun() Called when warmup is complete
---@param url? string
function M.warmup(callback, url)
	if M.is_loading_flag then
		return
	end

	local target_url = url or connection.get_active_url()
	if not target_url then
		return
	end

	M.is_loading_flag = true

	-- Safety net: if an async callback never fires (e.g. a hung job), clear the flag
	-- so future warmups aren't blocked forever.
	local done = false
	local function finish()
		if done then
			return
		end
		done = true
		M.is_loading_flag = false
		if callback then
			callback()
		end
	end
	vim.defer_fn(function()
		if M.is_loading_flag and not done then
			finish()
		end
	end, 30000)

	-- Load schemas first (async)
	schema.get_schemas_async(target_url, function(schemas, err)
		if err or #schemas == 0 then
			finish()
			return
		end

		-- Track pending schema loads
		local pending = 0
		local total = #schemas

		if total == 0 then
			finish()
			return
		end

		-- Load tables for each schema (async)
		for _, sch in ipairs(schemas) do
			schema.get_tables_async(target_url, sch.name, function(_, _)
				pending = pending + 1

				-- Check if all schemas are loaded
				if pending >= total then
					finish()
				end
			end)
		end
	end)
end

function M.invalidate()
	schema.clear_cache()
end

return M
