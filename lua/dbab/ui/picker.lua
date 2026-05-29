local Menu = require("nui.menu")
local event = require("nui.utils.autocmd").event
local connection = require("dbab.core.connection")
local history = require("dbab.core.history")

local M = {}

---@param on_select fun(item: Dbab.Connection|nil)
function M.open(on_select)
	local connections = connection.list_connections()

	if #connections == 0 then
		vim.notify("[dbab] No connections configured", vim.log.levels.WARN)
		on_select(nil)
		return
	end

	local lines = {}
	for _, conn in ipairs(connections) do
		local db_type = connection.parse_type(conn.url)
		local icon = M.get_icon(db_type)
		table.insert(lines, Menu.item(icon .. " " .. conn.name, { connection = conn }))
	end

	local menu = Menu({
		position = "50%",
		size = {
			width = 40,
			height = math.min(#connections + 2, 10),
		},
		border = {
			style = "rounded",
			text = {
				top = " Select Connection ",
				top_align = "center",
			},
		},
		win_options = {
			winhighlight = "Normal:Normal,FloatBorder:DbabBorder,CursorLine:DbabCellActive",
		},
	}, {
		lines = lines,
		max_width = 40,
		keymap = {
			focus_next = { "j", "<Down>", "<Tab>" },
			focus_prev = { "k", "<Up>", "<S-Tab>" },
			close = { "<Esc>", "<C-c>", "q" },
			submit = { "<CR>", "<Space>" },
		},
		on_submit = function(item)
			on_select(item.connection)
		end,
		on_close = function()
			on_select(nil)
		end,
	})

	menu:mount()

	menu:on(event.BufLeave, function()
		menu:unmount()
	end)
end

--- Open a picker to select a history query
---@param on_select fun(entry: Dbab.HistoryEntry|nil)
function M.open_history(on_select)
	history.load()
	local entries = history.get_all()

	if #entries == 0 then
		vim.notify("[dbab] No history yet", vim.log.levels.WARN)
		on_select(nil)
		return
	end

	local lines = {}
	local max_query = 60
	for _, entry in ipairs(entries) do
		local _, verb = history.format_summary(entry)
		local icon = history.get_verb_icon(verb)
		local time_str = os.date("%H:%M", entry.timestamp)
		local conn = entry.conn_name and ("[" .. entry.conn_name .. "] ") or ""
		local query = entry.query:gsub("%s+", " "):gsub("^%s+", ""):gsub("%s+$", "")
		if vim.fn.strdisplaywidth(query) > max_query then
			query = vim.fn.strcharpart(query, 0, max_query) .. "…"
		end
		local label = string.format("%s %s %s%s", icon, time_str, conn, query)
		table.insert(lines, Menu.item(label, { entry = entry }))
	end

	local menu = Menu({
		position = "50%",
		size = {
			width = 80,
			height = math.min(#entries + 2, 20),
		},
		border = {
			style = "rounded",
			text = {
				top = " Select History Query ",
				top_align = "center",
			},
		},
		win_options = {
			winhighlight = "Normal:Normal,FloatBorder:DbabBorder,CursorLine:DbabCellActive",
		},
	}, {
		lines = lines,
		max_width = 80,
		keymap = {
			focus_next = { "j", "<Down>", "<Tab>" },
			focus_prev = { "k", "<Up>", "<S-Tab>" },
			close = { "<Esc>", "<C-c>", "q" },
			submit = { "<CR>", "<Space>" },
		},
		on_submit = function(item)
			on_select(item.entry)
		end,
		on_close = function()
			on_select(nil)
		end,
	})

	menu:mount()

	menu:on(event.BufLeave, function()
		menu:unmount()
	end)
end

---@param db_type string
---@return string
function M.get_icon(db_type)
	local icons = require("dbab.ui.icons")
	return icons.db(db_type)
end

return M
