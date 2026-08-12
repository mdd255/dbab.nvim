local connection = require("dbab.core.connection")
local config = require("dbab.config")

local M = {}

local has_plenary, Job = pcall(require, "plenary.job")

local function use_dadbod()
	return config.get().executor == "dadbod"
end

--- Run `fn` with `env` vars set in vim.env, restoring previous values afterward.
--- Used so secrets (passwords) reach the child via env instead of argv.
---@param env table|nil
---@param fn fun(): any
---@return any
local function with_env(env, fn)
	if not env then
		return fn()
	end
	local saved = {}
	for k, v in pairs(env) do
		saved[k] = vim.env[k]
		vim.env[k] = v
	end
	local ok, res = pcall(fn)
	for k in pairs(env) do
		vim.env[k] = saved[k]
	end
	if not ok then
		error(res, 0)
	end
	return res
end

--- Merge `env` over a copy of the current process environment (for plenary Job).
---@param env table|nil
---@return table|nil
local function merged_env(env)
	if not env then
		return nil
	end
	local merged = vim.fn.environ()
	for k, v in pairs(env) do
		merged[k] = v
	end
	return merged
end

--- Strip ANSI color codes and Redis array index prefixes ("1) ") from a line.
---@param line string
---@return string
local function strip_redis_line(line)
	return line:gsub("\27%[[%d;]*m", ""):gsub("^%d+%)%s+", "")
end

--- Append the query itself to `list` for backends that take it as argv rather
--- than stdin (mongosh --eval, redis-cli/rdcli tokens).
---@param list string[]
---@param db_type string
---@param query string
local function append_query_args(list, db_type, query)
	if db_type == "mongodb" then
		table.insert(list, "--eval")
		table.insert(list, query)
	elseif db_type == "redis" then
		for _, token in ipairs(M._split_redis_args(query)) do
			table.insert(list, token)
		end
	end
end

-- ============================================
-- CLI backend
-- ============================================

---@param url string
---@param query string
---@return string
local function cli_execute(url, query)
	local adapter = require("dbab.core.adapter")
	local command, args, env = adapter.build_cmd(url)

	local cmd_list = { command }
	for _, arg in ipairs(args) do
		table.insert(cmd_list, arg)
	end

	local db_type = connection.parse_type(url)

	if db_type == "mongodb" or db_type == "redis" then
		append_query_args(cmd_list, db_type, query)

		local lines = with_env(env, function()
			return vim.fn.systemlist(cmd_list)
		end)

		if db_type == "mongodb" and vim.v.shell_error ~= 0 then
			vim.notify(
				"[dbab] mongosh error (exit " .. vim.v.shell_error .. "): " .. table.concat(lines, " "),
				vim.log.levels.WARN
			)
		elseif db_type == "redis" then
			for i, line in ipairs(lines) do
				lines[i] = strip_redis_line(line)
			end
		end

		return table.concat(lines, "\n")
	end

	-- Use list form to avoid shell expansion (e.g. '?' in URLs under zsh)
	local lines = with_env(env, function()
		return vim.fn.systemlist(cmd_list, query)
	end)

	return table.concat(lines, "\n")
end

--- Fallback for when plenary isn't installed: run the sync executor and report via callback.
---@param sync_fn fun(url: string, query: string): string
---@param url string
---@param query string
---@param callback fun(result: string, err: string|nil)
local function run_sync_as_async(sync_fn, url, query, callback)
	vim.schedule(function()
		local ok, result = pcall(sync_fn, url, query)
		if ok then
			callback(result, nil)
		else
			callback("", tostring(result))
		end
	end)
end

--- Run `command args` as a plenary Job, collecting stdout/stderr and invoking
--- `opts.callback(result, err)` on exit.
---@param command string
---@param args string[]
---@param opts { writer?: string, env?: table, filter_line?: fun(string): string, callback: fun(string, string|nil) }
local function run_job(command, args, opts)
	local stdout_results = {}
	local stderr_results = {}

	Job:new({
		command = command,
		args = args,
		writer = opts.writer,
		env = opts.env,
		on_stdout = function(_, data)
			if data then
				table.insert(stdout_results, opts.filter_line and opts.filter_line(data) or data)
			end
		end,
		on_stderr = function(_, data)
			if data then
				table.insert(stderr_results, data)
			end
		end,
		on_exit = function(_, return_val)
			vim.schedule(function()
				local result = table.concat(stdout_results, "\n")
				local err = #stderr_results > 0 and table.concat(stderr_results, "\n") or nil
				if return_val ~= 0 and err then
					opts.callback("", err)
				else
					opts.callback(result, nil)
				end
			end)
		end,
	}):start()
end

---@param url string
---@param query string
---@param callback fun(result: string, err: string|nil)
local function cli_execute_async(url, query, callback)
	if not has_plenary then
		return run_sync_as_async(cli_execute, url, query, callback)
	end

	local adapter = require("dbab.core.adapter")
	local command, args, env = adapter.build_cmd(url)
	local db_type = connection.parse_type(url)
	local is_redis = db_type == "redis"

	append_query_args(args, db_type, query)

	run_job(command, args, {
		-- mongo/redis already carry the query in argv; everything else uses stdin
		writer = (db_type ~= "mongodb" and not is_redis) and query or nil,
		env = merged_env(env),
		filter_line = is_redis and strip_redis_line or nil,
		callback = callback,
	})
end

-- ============================================
-- Dadbod backend
-- ============================================

---@param url string
---@return table|string|nil cmd
---@return boolean ok
local function dadbod_get_cmd(url)
	local ok, cmd = pcall(vim.fn["db#adapter#dispatch"], url, "interactive")

	if url:match("^mariadb://") then
		local dispatched_mariadb = ok
			and cmd
			and ((type(cmd) == "string" and cmd:match("^mariadb")) or (type(cmd) == "table" and cmd[1] == "mariadb"))

		-- vim-dadbod's mariadb adapter shells out to the `mariadb` binary; fall
		-- back to `mysql` (wire-compatible) when it's missing.
		if
			(dispatched_mariadb or not ok or not cmd)
			and vim.fn.executable("mariadb") == 0
			and vim.fn.executable("mysql") == 1
		then
			local fallback_url = url:gsub("^mariadb://", "mysql://")
			ok, cmd = pcall(vim.fn["db#adapter#dispatch"], fallback_url, "interactive")
		end
	end

	return cmd, ok
end

---@param url string
---@param query string
---@return string
local function dadbod_execute(url, query)
	local cmd = dadbod_get_cmd(url)
	local lines = vim.fn["db#systemlist"](cmd, query)
	return table.concat(lines, "\n")
end

---@param url string
---@param query string
---@param callback fun(result: string, err: string|nil)
local function dadbod_execute_async(url, query, callback)
	if not has_plenary then
		return run_sync_as_async(dadbod_execute, url, query, callback)
	end

	local cmd, ok = dadbod_get_cmd(url)
	if not ok or not cmd then
		vim.schedule(function()
			callback("", "Failed to get adapter command")
		end)
		return
	end

	local command, args
	if type(cmd) == "table" then
		command = cmd[1]
		args = vim.list_slice(cmd, 2)
	elseif type(cmd) == "string" then
		local parts = vim.split(cmd, " ")
		command = parts[1]
		args = vim.list_slice(parts, 2)
	else
		vim.schedule(function()
			callback("", "Unknown command format")
		end)
		return
	end

	run_job(command, args, { writer = query, callback = callback })
end

-- ============================================
-- Public API
-- ============================================

--- MongoDB shell queries must be prefixed with `db.` (e.g. `db.users.find()`).
--- Auto-prepend it when missing so users can type `users.find()` instead.
--- Only matches bare `identifier.method(...)` shorthand; leaves full scripts
--- (print(...), try {}, var ..., etc., as used by schema.lua) untouched.
---@param url string
---@param query string
---@return string
local function normalize_query(url, query)
	if connection.parse_type(url) == "mongodb" and query:match("^%s*[%a_][%w_]*%s*%.") and not query:match("^%s*db%.") then
		return "db." .. query
	end

	return query
end

---@param url string DB connection URL
---@param query string SQL query
---@return string result
function M.execute(url, query)
	query = normalize_query(url, query)

	local ok, result = pcall(function()
		if use_dadbod() then
			return dadbod_execute(url, query)
		end

		return cli_execute(url, query)
	end)

	if not ok then
		vim.notify("[dbab] Query execution failed: " .. tostring(result), vim.log.levels.ERROR)
		return ""
	end

	return result or ""
end

---@param query string SQL query
---@return string result
function M.execute_active(query)
	local url = connection.get_active_url()

	if not url then
		vim.notify("[dbab] No active connection. Use :Dbab connect first.", vim.log.levels.WARN)
		return ""
	end

	return M.execute(url, query)
end

---@param url string
---@param query string
---@param callback fun(result: string, err: string|nil)
function M.execute_async(url, query, callback)
	query = normalize_query(url, query)

	if use_dadbod() then
		dadbod_execute_async(url, query, callback)
	else
		cli_execute_async(url, query, callback)
	end
end

---@param query string SQL query
---@param callback fun(result: string, err: string|nil)
function M.execute_active_async(query, callback)
	local url = connection.get_active_url()

	if not url then
		vim.schedule(function()
			callback("", "No active connection")
		end)
		return
	end

	M.execute_async(url, query, callback)
end

--- Split a Redis command string into arguments, respecting quoted strings.
--- e.g. "SET foo 'hello world'" -> {"SET", "foo", "hello world"}
---@param query string
---@return string[]
function M._split_redis_args(query)
	local args = {}
	local i = 1
	local len = #query

	while i <= len do
		while i <= len and query:sub(i, i):match("%s") do
			i = i + 1
		end

		if i > len then
			break
		end

		local ch = query:sub(i, i)

		if ch == '"' or ch == "'" then
			local quote = ch
			i = i + 1
			local start = i

			while i <= len and query:sub(i, i) ~= quote do
				i = i + 1
			end

			table.insert(args, query:sub(start, i - 1))
			i = i + 1 -- skip closing quote
		else
			local start = i

			while i <= len and not query:sub(i, i):match("%s") do
				i = i + 1
			end

			table.insert(args, query:sub(start, i - 1))
		end
	end

	return args
end

return M
