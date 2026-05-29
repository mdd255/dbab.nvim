local M = {}

M.config = require("dbab.config")
M.core = require("dbab.core")
M.ui = require("dbab.ui")

---@param opts? Dbab.Config
function M.setup(opts)
	M.config.setup(opts)
	M.ui.highlights.setup()

	-- Register CMP source if nvim-cmp is available
	local has_cmp, cmp = pcall(require, "cmp")
	if has_cmp then
		cmp.register_source("dbab", require("cmp_dbab").new())
	end
end

--- Open the Workbench UI
function M.open()
	M.ui.workbench.open()
end

--- Open connection picker
function M.pick_connection()
	M.ui.picker.open(function(selected)
		if selected then
			M.core.connection.set_active(selected.name)
			vim.notify("[dbab] Connected to: " .. selected.name, vim.log.levels.INFO)
		end
	end)
end

--- Execute a query using active connection
---@param query string
---@return string
function M.execute(query)
	return M.core.executor.execute_active(query)
end

--- Set active connection by name
---@param name string
function M.connect(name)
	if M.core.connection.set_active(name) then
		vim.notify("[dbab] Connected to: " .. name, vim.log.levels.INFO)
	else
		vim.notify("[dbab] Connection not found: " .. name, vim.log.levels.ERROR)
	end
end

--- Disconnect a connection by name (defaults to the active one)
---@param name? string
function M.disconnect(name)
	name = name or M.core.connection.get_active_name()
	if not name then
		vim.notify("[dbab] No active connection to disconnect", vim.log.levels.WARN)
		return
	end
	if M.core.connection.disconnect(name) then
		vim.notify("[dbab] Disconnected: " .. name, vim.log.levels.INFO)
	else
		vim.notify("[dbab] Not connected: " .. name, vim.log.levels.WARN)
	end
end

--- List available connections
function M.list_connections()
	local connections = M.core.connection.list_connections()
	if #connections == 0 then
		vim.notify("[dbab] No connections configured", vim.log.levels.WARN)
		return
	end

	local lines = { "Available connections:" }
	for i, conn in ipairs(connections) do
		local active = conn.name == M.core.connection.get_active_name() and " (active)" or ""
		table.insert(lines, string.format("  %d. %s%s", i, conn.name, active))
	end
	vim.notify(table.concat(lines, "\n"), vim.log.levels.INFO)
end

--- Open the history query picker
function M.pick_history()
	M.ui.workbench.open_history_picker()
end

--- Hide or show the history window
function M.toggle_history()
	M.ui.workbench.toggle_history()
end

--- Close the workbench
function M.close()
	M.ui.workbench.close()
end

return M
