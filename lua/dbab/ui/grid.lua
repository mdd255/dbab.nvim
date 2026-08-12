local Popup = require("nui.popup")
local event = require("nui.utils.autocmd").event
local parser = require("dbab.utils.parser")
local config = require("dbab.config")

local M = {}

---@type table|nil
M.current_popup = nil

---@type Dbab.QueryResult|nil
M.current_result = nil

---@type number Current row (1-indexed)
M.cursor_row = 1

---@type number Current column (1-indexed)
M.cursor_col = 1

---@param cells string[]
---@param widths number[]
---@return string
local function pad_row(cells, widths)
	local line = ""

	for i, cell in ipairs(cells) do
		local w = widths[i] or #cell
		line = line .. " " .. cell .. string.rep(" ", w - #cell) .. " "
	end

	return line
end

---@param result Dbab.QueryResult
---@param widths number[]
---@return string[]
local function render_lines(result, widths)
	local lines = { pad_row(result.columns, widths) }

	for _, row in ipairs(result.rows) do
		table.insert(lines, pad_row(row, widths))
	end

	return lines
end

local function close_popup()
	if M.current_popup then
		M.current_popup:unmount()
		M.current_popup = nil
	end
end

---@param raw string Raw query result
---@param elapsed number Execution time in ms
function M.show(raw, elapsed)
	close_popup()

	local result = parser.parse(raw)
	M.current_result = result
	M.cursor_row = 1
	M.cursor_col = 1

	if #result.rows == 0 then
		vim.notify("[dbab] Query returned no data rows", vim.log.levels.INFO)
		return
	end

	local widths = parser.calculate_column_widths(result)
	local lines = render_lines(result, widths)

	local max_line_width = 0

	for _, line in ipairs(lines) do
		max_line_width = math.max(max_line_width, vim.fn.strdisplaywidth(line))
	end

	local opts = config.get()
	local width = math.min(max_line_width + 4, opts.result.max_width, vim.o.columns - 10)
	local height = math.min(#lines + 2, opts.result.max_height, vim.o.lines - 10)

	local popup = Popup({
		position = "50%",
		size = {
			width = width,
			height = height,
		},
		border = {
			style = "rounded",
			text = {
				top = string.format(" Result: %d rows (%.1fms) ", result.row_count, elapsed),
				top_align = "center",
				bottom = " q:close  j/k:scroll  y:yank row ",
				bottom_align = "center",
			},
		},
		win_options = {
			winhighlight = "Normal:Normal,FloatBorder:DbabBorder,CursorLine:DbabCellActive",
			cursorline = true,
		},
		buf_options = {
			modifiable = false,
			readonly = true,
			filetype = "dbab_result",
		},
	})

	M.current_popup = popup
	popup:mount()

	vim.api.nvim_buf_set_option(popup.bufnr, "modifiable", true)
	vim.api.nvim_buf_set_lines(popup.bufnr, 0, -1, false, lines)
	vim.api.nvim_buf_set_option(popup.bufnr, "modifiable", false)

	for line_num = 0, #lines - 1 do
		local row_hl = line_num % 2 == 0 and "DbabRowOdd" or "DbabRowEven"
		vim.api.nvim_buf_add_highlight(popup.bufnr, -1, row_hl, line_num, 0, -1)
	end

	-- Each header cell is rendered as " <col><padding> " (see pad_row); skip the
	-- leading space to highlight only the column name, then advance by cell + trailing space.
	local byte_pos = 0

	for i, col in ipairs(result.columns) do
		byte_pos = byte_pos + 1
		vim.api.nvim_buf_add_highlight(popup.bufnr, -1, "DbabHeader", 0, byte_pos, byte_pos + #col)
		byte_pos = byte_pos + widths[i] + 1
	end

	vim.api.nvim_win_set_cursor(popup.winid, { 2, 0 }) -- skip header row

	M.setup_keymaps(popup)

	popup:on(event.BufLeave, close_popup)
end

---@param popup table NuiPopup instance
function M.setup_keymaps(popup)
	local opts = { noremap = true, silent = true }

	popup:map("n", "q", close_popup, opts)
	popup:map("n", "<Esc>", close_popup, opts)
	popup:map("n", "y", M.yank_current_row, opts)
	popup:map("n", "Y", M.yank_all_rows, opts)
	popup:map("n", "c", M.yank_current_row_csv, opts)
end

---@param row string[]
---@param columns string[]
---@return table<string, string>
local function row_to_obj(row, columns)
	local obj = {}

	for i, col in ipairs(columns) do
		obj[col] = row[i]
	end

	return obj
end

---@return string[]|nil
local function get_selected_row()
	if not M.current_result or not M.current_popup then
		return nil
	end

	local cursor = vim.api.nvim_win_get_cursor(M.current_popup.winid)
	local row_idx = cursor[1] - 1 -- skip header row

	if row_idx < 1 or row_idx > #M.current_result.rows then
		vim.notify("[dbab] No data row selected", vim.log.levels.WARN)
		return nil
	end

	return M.current_result.rows[row_idx]
end

---@param text string
---@param label string
local function yank(text, label)
	vim.fn.setreg("+", text)
	vim.fn.setreg('"', text)
	vim.notify("[dbab] " .. label, vim.log.levels.INFO)
end

function M.yank_current_row()
	local row = get_selected_row()
	if not row then
		return
	end

	yank(vim.fn.json_encode(row_to_obj(row, M.current_result.columns)), "Row copied as JSON")
end

function M.yank_current_row_csv()
	local row = get_selected_row()
	if not row then
		return
	end

	yank(table.concat(row, ","), "Row copied as CSV")
end

function M.yank_all_rows()
	if not M.current_result then
		return
	end

	local arr = {}

	for _, row in ipairs(M.current_result.rows) do
		table.insert(arr, row_to_obj(row, M.current_result.columns))
	end

	yank(vim.fn.json_encode(arr), "All rows copied as JSON")
end

return M
