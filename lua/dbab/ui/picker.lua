local Menu = require("nui.menu")
local event = require("nui.utils.autocmd").event
local connection = require("dbab.core.connection")
local history = require("dbab.core.history")

local M = {}

local MENU_KEYMAP = {
	focus_next = { "j", "<Down>", "<Tab>" },
	focus_prev = { "k", "<Up>", "<S-Tab>" },
	close = { "<Esc>", "<C-c>", "q" },
	submit = { "<CR>", "<Space>" },
}

---@param opts { lines: table[], width: number, height: number, title: string, on_submit: fun(item), on_close: fun() }
local function open_menu(opts)
	local menu = Menu({
		position = "50%",
		size = { width = opts.width, height = opts.height },
		border = {
			style = "rounded",
			text = { top = opts.title, top_align = "center" },
		},
		win_options = {
			winhighlight = "Normal:Normal,FloatBorder:DbabBorder,CursorLine:DbabCellActive",
		},
	}, {
		lines = opts.lines,
		max_width = opts.width,
		keymap = MENU_KEYMAP,
		on_submit = opts.on_submit,
		on_close = opts.on_close,
	})

	menu:mount()

	menu:on(event.BufLeave, function()
		menu:unmount()
	end)
end

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
		local icon = M.get_icon(connection.parse_type(conn.url))
		table.insert(lines, Menu.item(icon .. " " .. conn.name, { connection = conn }))
	end

	open_menu({
		lines = lines,
		width = 40,
		height = math.min(#connections + 2, 10),
		title = " Select Connection ",
		on_submit = function(item)
			on_select(item.connection)
		end,
		on_close = function()
			on_select(nil)
		end,
	})
end

---@param on_select fun(entry: Dbab.HistoryEntry|nil)
function M.open_history(on_select)
	history.load()
	local all_entries = history.get_all()

	-- Filter to current active connection
	local conn_name = connection.get_active_name()
	local entries = all_entries

	if conn_name then
		entries = vim.tbl_filter(function(entry)
			return entry.conn_name == conn_name
		end, all_entries)
	end

	if #entries == 0 then
		vim.notify("[dbab] No history yet", vim.log.levels.WARN)
		on_select(nil)
		return
	end

	local lines = {}
	local max_query = 72

	for _, entry in ipairs(entries) do
		local query = entry.query:gsub("%s+", " "):gsub("^%s+", ""):gsub("%s+$", "")

		if vim.fn.strdisplaywidth(query) > max_query then
			query = vim.fn.strcharpart(query, 0, max_query) .. "…"
		end

		table.insert(lines, Menu.item(query, { entry = entry }))
	end

	open_menu({
		lines = lines,
		width = 80,
		height = math.min(#entries + 2, 20),
		title = " Select History Query ",
		on_submit = function(item)
			on_select(item.entry)
		end,
		on_close = function()
			on_select(nil)
		end,
	})
end

---@param db_type string
---@return string
function M.get_icon(db_type)
	local icons = require("dbab.ui.icons")
	return icons.db(db_type)
end

return M
