local connection = require("dbab.core.connection")
local config = require("dbab.config")

local M = {}

local has_plenary, Job = pcall(require, "plenary.job")

local function use_dadbod()
  return config.get().executor == "dadbod"
end

-- ============================================
-- CLI backend
-- ============================================

---@param url string
---@param query string
---@return string
local function cli_execute(url, query)
  local adapter = require("dbab.core.adapter")
  local command, args = adapter.build_cmd(url)

  local cmd_list = { command }
  for _, arg in ipairs(args) do
    table.insert(cmd_list, arg)
  end

  -- MongoDB: pass query via --eval to avoid REPL prompt noise
  if connection.parse_type(url) == "mongodb" then
    table.insert(cmd_list, "--eval")
    table.insert(cmd_list, query)
    local lines = vim.fn.systemlist(cmd_list)
    if vim.v.shell_error ~= 0 then
      vim.notify("[dbab] mongosh error (exit " .. vim.v.shell_error .. "): " .. table.concat(lines, " "), vim.log.levels.WARN)
    end
    return table.concat(lines, "\n")
  end

  -- Redis: pass command as arguments (redis-cli / rdcli don't read from stdin reliably)
  if connection.parse_type(url) == "redis" then
    -- Split query into words, respecting quoted strings
    for _, token in ipairs(M._split_redis_args(query)) do
      table.insert(cmd_list, token)
    end
    local lines = vim.fn.systemlist(cmd_list)
    for i, line in ipairs(lines) do
      -- Strip ANSI color codes (rdcli outputs colored text)
      line = line:gsub("\27%[[%d;]*m", "")
      -- Strip Redis array index prefix: "1) " -> ""
      line = line:gsub("^%d+%)%s+", "")
      lines[i] = line
    end
    return table.concat(lines, "\n")
  end

  -- Use list form to avoid shell expansion (e.g. '?' in URLs under zsh)
  local lines = vim.fn.systemlist(cmd_list, query)
  return table.concat(lines, "\n")
end

---@param url string
---@param query string
---@param callback fun(result: string, err: string|nil)
local function cli_execute_async(url, query, callback)
  if not has_plenary then
    vim.schedule(function()
      local ok, result = pcall(cli_execute, url, query)
      if ok then
        callback(result, nil)
      else
        callback("", tostring(result))
      end
    end)
    return
  end

  local adapter = require("dbab.core.adapter")
  local command, args = adapter.build_cmd(url)

  -- MongoDB: pass query via --eval to avoid REPL prompt noise
  local db_type = connection.parse_type(url)
  local is_mongodb = db_type == "mongodb"
  local is_redis = db_type == "redis"
  if is_mongodb then
    table.insert(args, "--eval")
    table.insert(args, query)
  elseif is_redis then
    for _, token in ipairs(M._split_redis_args(query)) do
      table.insert(args, token)
    end
  end

  local stdout_results = {}
  local stderr_results = {}

  local job_opts = {
    command = command,
    args = args,
    on_stdout = function(_, data)
      if data then
        if is_redis then
          -- Strip ANSI color codes (rdcli outputs colored text)
          data = data:gsub("\27%[[%d;]*m", "")
          -- Strip Redis array index prefix: "1) " -> ""
          data = data:gsub("^%d+%)%s+", "")
        end
        table.insert(stdout_results, data)
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
          callback("", err)
        else
          callback(result, nil)
        end
      end)
    end,
  }
  if not is_mongodb and not is_redis then
    job_opts.writer = query
  end
  Job:new(job_opts):start()
end

-- ============================================
-- Dadbod backend
-- ============================================

---@param url string
---@return table|string|nil cmd
---@return boolean ok
local function dadbod_get_cmd(url)
  local ok, cmd = pcall(vim.fn["db#adapter#dispatch"], url, "interactive")

  if ok and cmd and url:match("^mariadb://") then
    local is_mariadb = false
    if type(cmd) == "string" and cmd:match("^mariadb") then
      is_mariadb = true
    end
    if type(cmd) == "table" and cmd[1] == "mariadb" then
      is_mariadb = true
    end

    if is_mariadb and vim.fn.executable("mariadb") == 0 and vim.fn.executable("mysql") == 1 then
      local fallback_url = url:gsub("^mariadb://", "mysql://")
      ok, cmd = pcall(vim.fn["db#adapter#dispatch"], fallback_url, "interactive")
    end
  end

  if (not ok or not cmd) and url:match("^mariadb://") then
    if vim.fn.executable("mariadb") == 0 and vim.fn.executable("mysql") == 1 then
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
    vim.schedule(function()
      local ok, result = pcall(dadbod_execute, url, query)
      if ok then
        callback(result, nil)
      else
        callback("", tostring(result))
      end
    end)
    return
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

  local stdout_results = {}
  local stderr_results = {}

  Job:new({
    command = command,
    args = args,
    writer = query,
    on_stdout = function(_, data)
      if data then
        table.insert(stdout_results, data)
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
          callback("", err)
        else
          callback(result, nil)
        end
      end)
    end,
  }):start()
end

-- ============================================
-- Public API
-- ============================================

---@param url string DB connection URL
---@param query string SQL query
---@return string result
function M.execute(url, query)
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
    -- Skip whitespace
    while i <= len and query:sub(i, i):match("%s") do
      i = i + 1
    end
    if i > len then
      break
    end
    local ch = query:sub(i, i)
    if ch == '"' or ch == "'" then
      -- Quoted string
      local quote = ch
      i = i + 1
      local start = i
      while i <= len and query:sub(i, i) ~= quote do
        i = i + 1
      end
      table.insert(args, query:sub(start, i - 1))
      i = i + 1 -- skip closing quote
    else
      -- Unquoted token
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
