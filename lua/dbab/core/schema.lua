local executor = require("dbab.core.executor")
local connection = require("dbab.core.connection")
local config = require("dbab.config")

local M = {}

--- Escape a SQL string literal (single quotes) for safe interpolation.
--- Identifiers from the DB can contain quotes; without escaping the query breaks.
---@param s string
---@return string
local function sql_escape(s)
	return (s:gsub("'", "''"))
end

--- Escape a value for embedding in a double-quoted JS string (mongosh --eval).
---@param s string
---@return string
local function js_escape(s)
	return (s:gsub("\\", "\\\\"):gsub('"', '\\"'))
end

-- In-memory cache for schema data
local cache = { by_url = {} }

---@param url string
---@return table
local function get_url_cache(url)
	if not cache.by_url[url] then
		cache.by_url[url] = { schemas = nil, tables = {}, columns = {} }
	end
	return cache.by_url[url]
end

--- Clear cache (call when connection changes)
---@param url? string
function M.clear_cache(url)
	if url then
		cache.by_url[url] = nil
		return
	end
	cache.by_url = {}
end

--- Get cached table names only (NO DB queries, for CMP)
---@param url string
---@return string[]
function M.get_cached_table_names(url)
	local url_cache = cache.by_url[url]
	if not url_cache then
		return {}
	end

	local names, seen = {}, {}
	for _, tables in pairs(url_cache.tables) do
		for _, tbl in ipairs(tables) do
			if not seen[tbl.name] then
				seen[tbl.name] = true
				table.insert(names, tbl.name)
			end
		end
	end
	return names
end

--- Get cached columns only (NO DB queries, for CMP)
---@param url string
---@return Dbab.Column[]
function M.get_cached_columns(url)
	local url_cache = cache.by_url[url]
	if not url_cache then
		return {}
	end

	local all, seen = {}, {}
	for _, columns in pairs(url_cache.columns) do
		for _, col in ipairs(columns) do
			if not seen[col.name] then
				seen[col.name] = true
				table.insert(all, col)
			end
		end
	end
	return all
end

--- Check if cache has data
---@param url string
---@return boolean
function M.has_cache(url)
	local url_cache = cache.by_url[url]
	return url_cache ~= nil and url_cache.schemas ~= nil
end

-- See lua/dbab/types.lua for type definitions (Dbab.Schema, Dbab.Table, Dbab.Column)

--- Build the schema-listing query for a db type.
--- Returns (early_result, query): early_result is set when the db type has no real
--- schema concept (sqlite/redis) and no query is needed; query is set otherwise.
--- Both nil means the db type is unsupported.
---@param db_type string
---@param opts Dbab.Config
---@return Dbab.Schema[]|nil, string|nil
local function build_schemas_query(db_type, opts)
	if db_type == "sqlite" then
		return { { name = "main", table_count = 0 } }
	elseif db_type == "redis" then
		return { { name = "default", table_count = 0 } }
	elseif db_type == "postgres" then
		local exclude_list = "'pg_toast', 'pg_temp_1', 'pg_toast_temp_1'"
		if not opts.sidebar.show_system_schemas then
			exclude_list = exclude_list .. ", 'information_schema', 'pg_catalog'"
		end
		return nil,
			string.format(
				[[
      SELECT schema_name,
             (SELECT COUNT(*) FROM information_schema.tables t WHERE t.table_schema = s.schema_name) as table_count
      FROM information_schema.schemata s
      WHERE schema_name NOT IN (%s)
      ORDER BY
        CASE WHEN schema_name = 'public' THEN 0 ELSE 1 END,
        schema_name
    ]],
				exclude_list
			)
	elseif db_type == "mysql" then
		return nil,
			[[
      SELECT schema_name,
             (SELECT COUNT(*) FROM information_schema.tables t WHERE t.table_schema = s.schema_name) as table_count
      FROM information_schema.schemata s
      WHERE schema_name = DATABASE()
    ]]
	elseif db_type == "mongodb" then
		return nil, 'print("schema_name\\ttable_count"); print("default\\t" + db.getCollectionNames().length);'
	end
	return nil, nil
end

---@param url string
---@return Dbab.Schema[]
function M.get_schemas(url)
	local url_cache = get_url_cache(url)
	if url_cache.schemas then
		return url_cache.schemas
	end

	local early, query = build_schemas_query(connection.parse_type(url), config.get())
	if early then
		url_cache.schemas = early
	elseif query then
		url_cache.schemas = M.parse_schemas(executor.execute(url, query))
	else
		return {}
	end
	return url_cache.schemas
end

---@param url string
---@param callback fun(schemas: Dbab.Schema[], err: string|nil)
function M.get_schemas_async(url, callback)
	local url_cache = get_url_cache(url)
	if url_cache.schemas then
		vim.schedule(function()
			callback(url_cache.schemas, nil)
		end)
		return
	end

	local early, query = build_schemas_query(connection.parse_type(url), config.get())
	if early then
		url_cache.schemas = early
		vim.schedule(function()
			callback(early, nil)
		end)
		return
	elseif not query then
		vim.schedule(function()
			callback({}, nil)
		end)
		return
	end

	executor.execute_async(url, query, function(result, err)
		if err then
			callback({}, err)
			return
		end
		url_cache.schemas = M.parse_schemas(result)
		callback(url_cache.schemas, nil)
	end)
end

--- Detect if tabular text is tab-separated (MySQL) rather than pipe-bordered (psql/sqlite CLI).
---@param lines string[]
---@return boolean
local function is_tab_separated(lines)
	return lines[1] ~= nil and lines[1]:find("\t") ~= nil
end

--- Find the first data row of a pipe-bordered result table, skipping the
--- header and its `---+---`/`───` separator line.
---@param lines string[]
---@return number
local function find_pipe_data_start(lines)
	for i, line in ipairs(lines) do
		if line:match("^%-") or line:match("^%+") or line:match("^─") then
			return i + 1
		end
	end
	return 1
end

---@param raw string
---@return Dbab.Schema[]
function M.parse_schemas(raw)
	local schemas = {}
	local lines = vim.split(raw, "\n")

	if is_tab_separated(lines) then
		for i = 2, #lines do
			local parts = lines[i] ~= "" and vim.split(lines[i], "\t") or nil
			if parts and #parts >= 2 then
				local name = vim.trim(parts[1])
				if name ~= "" then
					table.insert(schemas, { name = name, table_count = tonumber(vim.trim(parts[2])) or 0 })
				end
			end
		end
	else
		for i = find_pipe_data_start(lines), #lines do
			local line = vim.trim(lines[i])
			if line ~= "" and not line:match("^%(") and not line:match("rows%)") then
				local parts = vim.split(line, "|")
				if #parts >= 2 then
					local name = vim.trim(parts[1])
					if name ~= "" then
						table.insert(schemas, { name = name, table_count = tonumber(vim.trim(parts[2])) or 0 })
					end
				end
			end
		end
	end

	return schemas
end

--- Build the table-listing query for a db type. Returns nil if unsupported.
---@param db_type string
---@param schema_name string
---@return string|nil
local function build_tables_query(db_type, schema_name)
	if db_type == "postgres" then
		return string.format(
			[[
      SELECT table_name, table_type
      FROM information_schema.tables
      WHERE table_schema = '%s'
      ORDER BY table_type, table_name
    ]],
			sql_escape(schema_name)
		)
	elseif db_type == "mysql" then
		return [[
      SELECT table_name, table_type
      FROM information_schema.tables
      WHERE table_schema = DATABASE()
      ORDER BY table_type, table_name
    ]]
	elseif db_type == "sqlite" then
		return [[
      SELECT name as table_name, type as table_type
      FROM sqlite_master
      WHERE type IN ('table', 'view')
      ORDER BY type, name
    ]]
	elseif db_type == "mongodb" then
		return 'print("table_name\\ttable_type"); db.getCollectionNames().sort().forEach(function(c) { print(c + "\\tcollection"); });'
	elseif db_type == "redis" then
		return "KEYS *"
	end
	return nil
end

---@param result string
---@param db_type string
---@return Dbab.Table[]
local function parse_tables_result(result, db_type)
	if db_type == "redis" then
		return M.parse_redis_keys(result)
	end
	return M.parse_tables(result, db_type)
end

---@param url string
---@param schema_name? string
---@return Dbab.Table[]
function M.get_tables(url, schema_name)
	schema_name = schema_name or "public"
	local url_cache = get_url_cache(url)
	if url_cache.tables[schema_name] then
		return url_cache.tables[schema_name]
	end

	local db_type = connection.parse_type(url)
	local query = build_tables_query(db_type, schema_name)
	if not query then
		return {}
	end

	url_cache.tables[schema_name] = parse_tables_result(executor.execute(url, query), db_type)
	return url_cache.tables[schema_name]
end

---@param url string
---@param schema_name string
---@param callback fun(tables: Dbab.Table[], err: string|nil)
function M.get_tables_async(url, schema_name, callback)
	schema_name = schema_name or "public"
	local url_cache = get_url_cache(url)
	if url_cache.tables[schema_name] then
		vim.schedule(function()
			callback(url_cache.tables[schema_name], nil)
		end)
		return
	end

	local db_type = connection.parse_type(url)
	local query = build_tables_query(db_type, schema_name)
	if not query then
		vim.schedule(function()
			callback({}, nil)
		end)
		return
	end

	executor.execute_async(url, query, function(result, err)
		if err then
			callback({}, err)
			return
		end
		url_cache.tables[schema_name] = parse_tables_result(result, db_type)
		callback(url_cache.tables[schema_name], nil)
	end)
end

---@param raw string
---@param db_type string
---@return Dbab.Table[]
function M.parse_tables(raw, db_type)
	local tables = {}
	local lines = vim.split(raw, "\n")

	if is_tab_separated(lines) then
		for i = 2, #lines do
			local parts = lines[i] ~= "" and vim.split(lines[i], "\t") or nil
			if parts and #parts >= 2 then
				local name = vim.trim(parts[1])
				if name ~= "" then
					local table_type = vim.trim(parts[2]):upper():match("VIEW") and "view" or "table"
					table.insert(tables, { name = name, type = table_type })
				end
			end
		end
	else
		for i = find_pipe_data_start(lines), #lines do
			local line = vim.trim(lines[i])
			if line ~= "" and not line:match("^%(") and not line:match("rows%)") then
				local name, ttype
				if db_type == "postgres" then
					-- e.g. " table_name | BASE TABLE" or " view_name | VIEW"
					name, ttype = line:match("^%s*([%w_]+)%s*|%s*(.+)%s*$")
				else
					local parts = vim.split(line, "|")
					if #parts >= 2 then
						name, ttype = vim.trim(parts[1]), vim.trim(parts[2])
					end
				end

				if name and name ~= "" then
					local table_type = (ttype and ttype:upper():match("VIEW")) and "view" or "table"
					table.insert(tables, { name = name, type = table_type })
				end
			end
		end
	end

	return tables
end

--- Build the column-listing query for a db type. Returns nil if the db type
--- has no column introspection (redis) or is unsupported.
---@param db_type string
---@param table_name string
---@return string|nil
local function build_columns_query(db_type, table_name)
	if db_type == "postgres" then
		return string.format(
			[[
      SELECT
        c.column_name,
        c.data_type,
        c.is_nullable,
        CASE WHEN pk.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END as is_primary
      FROM information_schema.columns c
      LEFT JOIN (
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = '%s' AND tc.constraint_type = 'PRIMARY KEY'
      ) pk ON c.column_name = pk.column_name
      WHERE c.table_name = '%s' AND c.table_schema = 'public'
      ORDER BY c.ordinal_position
    ]],
			sql_escape(table_name),
			sql_escape(table_name)
		)
	elseif db_type == "mysql" then
		return string.format(
			[[
      SELECT
        column_name,
        data_type,
        is_nullable,
        CASE WHEN column_key = 'PRI' THEN 'YES' ELSE 'NO' END as is_primary
      FROM information_schema.columns
      WHERE table_name = '%s' AND table_schema = DATABASE()
      ORDER BY ordinal_position
    ]],
			sql_escape(table_name)
		)
	elseif db_type == "sqlite" then
		return string.format("PRAGMA table_info('%s')", sql_escape(table_name))
	elseif db_type == "mongodb" then
		return string.format(
			[[
try {
  print("column_name\tdata_type\tis_nullable\tis_primary");
  var doc = db.getCollection("%s").findOne();
  if (doc) {
    Object.keys(doc).forEach(function(k) {
      var v = doc[k];
      var t = typeof v;
      if (v === null || v === undefined) { t = "null"; }
      else if (Array.isArray(v)) { t = "array"; }
      else if (t === "object") {
        try { t = v.constructor.name.toLowerCase(); } catch(e) { t = "object"; }
      }
      print(k + "\t" + t + "\tYES\t" + (k === "_id" ? "YES" : "NO"));
    });
  }
} catch(err) {
  print("column_name\tdata_type\tis_nullable\tis_primary");
  print("_error\t" + err.message + "\tNO\tNO");
}
]],
			js_escape(table_name)
		)
	end
	return nil
end

---@param url string
---@param table_name string
---@return Dbab.Column[]
function M.get_columns(url, table_name)
	local url_cache = get_url_cache(url)
	if url_cache.columns[table_name] then
		return url_cache.columns[table_name]
	end

	local db_type = connection.parse_type(url)
	local query = build_columns_query(db_type, table_name)
	if not query then
		url_cache.columns[table_name] = {}
		return url_cache.columns[table_name]
	end

	url_cache.columns[table_name] = M.parse_columns(executor.execute(url, query), db_type)
	return url_cache.columns[table_name]
end

---@param url string
---@param table_name string
---@param callback fun(columns: Dbab.Column[], err: string|nil)
function M.get_columns_async(url, table_name, callback)
	local url_cache = get_url_cache(url)
	if url_cache.columns[table_name] then
		vim.schedule(function()
			callback(url_cache.columns[table_name], nil)
		end)
		return
	end

	local db_type = connection.parse_type(url)
	local query = build_columns_query(db_type, table_name)
	if not query then
		url_cache.columns[table_name] = {}
		vim.schedule(function()
			callback(url_cache.columns[table_name], nil)
		end)
		return
	end

	executor.execute_async(url, query, function(result, err)
		if err then
			callback({}, err)
			return
		end
		url_cache.columns[table_name] = M.parse_columns(result, db_type)
		callback(url_cache.columns[table_name], nil)
	end)
end

---@param raw string
---@param db_type string
---@return Dbab.Column[]
function M.parse_columns(raw, db_type)
	local columns = {}
	local lines = vim.split(raw, "\n")

	if is_tab_separated(lines) then
		for i = 2, #lines do
			local parts = lines[i] ~= "" and vim.split(lines[i], "\t") or nil
			if parts and #parts >= 4 then
				local col = {
					name = vim.trim(parts[1]),
					data_type = vim.trim(parts[2]),
					is_nullable = vim.trim(parts[3]):upper() == "YES",
					is_primary = vim.trim(parts[4]):upper() == "YES",
				}
				if col.name ~= "" then
					table.insert(columns, col)
				end
			end
		end
	else
		for i = find_pipe_data_start(lines), #lines do
			local line = vim.trim(lines[i])
			if line ~= "" and not line:match("^%(") and not line:match("rows%)") then
				local col = {}
				local parts = vim.split(line, "|")

				if db_type == "sqlite" then
					-- PRAGMA table_info: cid|name|type|notnull|dflt_value|pk
					if #parts >= 6 then
						col.name = vim.trim(parts[2])
						col.data_type = vim.trim(parts[3])
						col.is_nullable = vim.trim(parts[4]) == "0"
						col.is_primary = vim.trim(parts[6]) == "1"
					end
				elseif #parts >= 4 then
					col.name = vim.trim(parts[1])
					col.data_type = vim.trim(parts[2])
					col.is_nullable = vim.trim(parts[3]):upper() == "YES"
					col.is_primary = vim.trim(parts[4]):upper() == "YES"
				end

				if col.name and col.name ~= "" then
					table.insert(columns, col)
				end
			end
		end
	end

	return columns
end

---@param raw string
---@return Dbab.Table[]
function M.parse_redis_keys(raw)
	local tables = {}

	for _, raw_line in ipairs(vim.split(raw, "\n")) do
		local line = vim.trim(raw_line)
		if line ~= "" and line ~= "(empty array)" and line ~= "(empty list or set)" then
			-- Numbered format ("1) keyname"/"1) \"keyname\"") or a bare/quoted key.
			local key = line:match('^%d+%)%s+"(.+)"$') or line:match("^%d+%)%s+(.+)$") or line:match('^"(.+)"$') or line
			if key ~= "" then
				table.insert(tables, { name = key, type = "key" })
			end
		end
	end

	table.sort(tables, function(a, b)
		return a.name < b.name
	end)
	return tables
end

return M
