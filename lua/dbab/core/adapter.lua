local connection = require("dbab.core.connection")

local M = {}

--- Percent-decode a URL component (e.g. "%40" -> "@", "+" -> space).
---@param s string
---@return string
local function url_decode(s)
	s = s:gsub("+", " ")
	s = s:gsub("%%(%x%x)", function(hex)
		return string.char(tonumber(hex, 16))
	end)
	return s
end

--- Remove ":password@" from a URL so the secret never lands in argv (visible via `ps`).
---@param url string
---@param password string|nil
---@return string
local function strip_password(url, password)
	if not password or password == "" then
		return url
	end
	local needle = ":" .. password .. "@"
	local idx = url:find(needle, 1, true)
	if idx then
		return url:sub(1, idx - 1) .. "@" .. url:sub(idx + #needle)
	end
	return url
end

---@param url string
---@return table parsed { scheme, user, password, host, port, database, params }
function M.parse_url(url)
	local result = {}

	local scheme = url:match("^([%w]+)://")
	result.scheme = scheme or "unknown"

	if result.scheme == "sqlite" then
		local path = url:match("^sqlite:///(.+)") or url:match("^sqlite://(.+)")
		if path then
			if path:match("^/") or path:match("^%w+:") or path:match("^~") then
				result.database = path
			elseif
				path:match("^home/")
				or path:match("^Users/")
				or path:match("^tmp/")
				or path:match("^var/")
				or path:match("^etc/")
				or path:match("^usr/")
			then
				result.database = "/" .. path
			else
				result.database = path
			end
		else
			result.database = ""
		end
		return result
	end

	local rest = url:gsub("^[%w]+://", "")

	local query_string
	rest, query_string = rest:match("^(.-)%?(.+)$")
	if not rest then
		rest = url:gsub("^[%w]+://", "")
	end

	if query_string then
		result.params = {}
		for pair in query_string:gmatch("[^&]+") do
			local key, value = pair:match("^([^=]+)=(.*)$")
			if key then
				result.params[url_decode(key)] = url_decode(value)
			else
				-- valueless param, e.g. "?sslmode" -> {sslmode = ""}
				result.params[url_decode(pair)] = ""
			end
		end
	end

	local auth, hostpath = rest:match("^(.+)@(.+)$")
	if auth then
		local user, password = auth:match("^([^:]+):(.+)$")
		if user then
			result.user = user
			result.password = password
		else
			result.user = auth
		end
		rest = hostpath
	end

	local hostport, database = rest:match("^([^/]+)/(.+)$")
	if hostport then
		local host, port = hostport:match("^(.+):(%d+)$")
		if host then
			result.host = host
			result.port = port
		else
			result.host = hostport
		end
		result.database = database
	else
		local host, port = rest:match("^(.+):(%d+)$")
		if host then
			result.host = host
			result.port = port
		else
			result.host = rest
		end
	end

	return result
end

---@param url string
---@return string command
---@return string[] args
function M.build_cmd(url)
	local db_type = connection.parse_type(url)

	if db_type == "postgres" then
		return M._build_postgres(url)
	elseif db_type == "mysql" then
		return M._build_mysql(url)
	elseif db_type == "sqlite" then
		return M._build_sqlite(url)
	elseif db_type == "mongodb" then
		return M._build_mongodb(url)
	elseif db_type == "redis" then
		return M._build_redis(url)
	end

	error("Unsupported database type: " .. db_type)
end

---@param url string
---@return string command, string[] args, table|nil env
function M._build_postgres(url)
	local parsed = M.parse_url(url)
	if parsed.password and parsed.password ~= "" then
		return "psql", { strip_password(url, parsed.password) }, { PGPASSWORD = parsed.password }
	end
	return "psql", { url }
end

---@param url string
---@return string command, string[] args
function M._build_mysql(url)
	local parsed = M.parse_url(url)
	local command = "mysql"
	local args = {}

	if url:match("^mariadb://") then
		if vim.fn.executable("mariadb") == 1 then
			command = "mariadb"
		elseif vim.fn.executable("mysql") == 1 then
			command = "mysql"
		end
	end

	if parsed.params and parsed.params["login-path"] then
		table.insert(args, "--login-path=" .. parsed.params["login-path"])
	end

	if parsed.host then
		table.insert(args, "-h")
		table.insert(args, parsed.host)
	end
	if parsed.port then
		table.insert(args, "-P")
		table.insert(args, parsed.port)
	end
	if parsed.user then
		table.insert(args, "-u")
		table.insert(args, parsed.user)
	end
	if parsed.database then
		table.insert(args, parsed.database)
	end

	-- Pass password via MYSQL_PWD env, not -p<pw> argv (which leaks via `ps`).
	local env
	if parsed.password and parsed.password ~= "" then
		env = { MYSQL_PWD = parsed.password }
	end

	return command, args, env
end

---@param url string
---@return string command, string[] args
function M._build_sqlite(url)
	local parsed = M.parse_url(url)
	return "sqlite3", { parsed.database }
end

---@param url string
---@return string command, string[] args
function M._build_mongodb(url)
	return "mongosh", { url, "--quiet", "--norc" }
end

---@param url string
---@return string command, string[] args
function M._build_redis(url)
	local parsed = M.parse_url(url)
	local config = require("dbab.config")
	local redis_cmd = config.get().redis and config.get().redis.command or "redis-cli"
	local args = {}

	if parsed.host then
		table.insert(args, "-h")
		table.insert(args, parsed.host)
	end
	if parsed.port then
		table.insert(args, "-p")
		table.insert(args, parsed.port)
	end
	-- Pass auth via REDISCLI_AUTH env, not -a <pw> argv (which leaks via `ps`).
	local env
	if parsed.password and parsed.password ~= "" then
		env = { REDISCLI_AUTH = parsed.password }
		table.insert(args, "--no-auth-warning")
	end
	if parsed.database and parsed.database ~= "" then
		table.insert(args, "-n")
		table.insert(args, parsed.database)
	end

	return redis_cmd, args, env
end

return M
