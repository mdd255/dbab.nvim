--- History panel UI: renders the history buffer/window in the bottom-left quadrant
local history = require("dbab.core.history")
local config = require("dbab.config")
local connection = require("dbab.core.connection")
local icons = require("dbab.ui.icons")

local M = {}

---@return string|nil
local function get_current_connection_name()
	local ok, workbench = pcall(require, "dbab.ui.workbench")

	if ok and workbench and workbench.get_active_connection_context then
		local conn_name = workbench.get_active_connection_context()

		if conn_name then
			return conn_name
		end
	end

	return connection.get_active_name()
end

---@type number|nil
M.buf = nil

---@type number|nil
M.win = nil

---@type table[] entry_line_map: {{start=N, finish=N}, ...} (1-indexed line numbers)
M.entry_line_map = {}

--- Parse `query_text` as SQL and return its root node plus the SQL highlights query
---@param query_text string
---@return TSNode|nil root
---@return vim.treesitter.Query|nil hl_query
local function parse_sql_highlights(query_text)
	local ok, ts_parser = pcall(vim.treesitter.get_string_parser, query_text, "sql")

	if not ok or not ts_parser then
		return nil, nil
	end

	local tree = ts_parser:parse()[1]

	if not tree then
		return nil, nil
	end

	local query_ok, hl_query = pcall(vim.treesitter.query.get, "sql", "highlights")

	if not query_ok or not hl_query then
		return nil, nil
	end

	return tree:root(), hl_query
end

--- Apply treesitter SQL syntax highlighting to `query_text` rendered starting at buffer
--- line `start_line`. For a single-line query, pass the same offset for both params.
---@param buf number Buffer number
---@param ns number Namespace id
---@param start_line number First buffer line (0-indexed) where the query starts
---@param first_line_offset number Column offset for the first line
---@param other_line_offset number Column offset for subsequent lines
---@param query_text string The SQL query text to highlight
local function apply_treesitter_highlights(buf, ns, start_line, first_line_offset, other_line_offset, query_text)
	local root, hl_query = parse_sql_highlights(query_text)

	if not root then
		return
	end

	local query_lines = vim.split(query_text, "\n")

	for id, node in hl_query:iter_captures(root, query_text, 0, -1) do
		local name = hl_query.captures[id]
		local start_row, start_col, end_row, end_col = node:range()
		local hl_group = "@" .. name .. ".sql"

		if start_row == end_row then
			local offset = start_row == 0 and first_line_offset or other_line_offset

			pcall(
				vim.api.nvim_buf_add_highlight,
				buf,
				ns,
				hl_group,
				start_line + start_row,
				offset + start_col,
				offset + end_col
			)
		else
			for row = start_row, end_row do
				local offset = row == 0 and first_line_offset or other_line_offset
				local s_col, e_col

				if row == start_row then
					s_col = start_col
					e_col = #(query_lines[row + 1] or "")
				elseif row == end_row then
					s_col = 0
					e_col = end_col
				else
					s_col = 0
					e_col = #(query_lines[row + 1] or "")
				end

				pcall(vim.api.nvim_buf_add_highlight, buf, ns, hl_group, start_line + row, offset + s_col, offset + e_col)
			end
		end
	end
end

---@return number buf
function M.get_or_create_buf()
	if M.buf and vim.api.nvim_buf_is_valid(M.buf) then
		return M.buf
	end

	M.buf = vim.api.nvim_create_buf(false, true)
	vim.api.nvim_buf_set_option(M.buf, "filetype", "dbab_history")
	vim.api.nvim_buf_set_option(M.buf, "buftype", "nofile")
	vim.api.nvim_buf_set_option(M.buf, "bufhidden", "hide")
	vim.api.nvim_buf_set_option(M.buf, "swapfile", false)
	vim.api.nvim_buf_set_option(M.buf, "modifiable", false)
	vim.api.nvim_buf_set_name(M.buf, "History")

	return M.buf
end

local CONN_NAME_WIDTH = 8

--- Truncate/pad a connection name to CONN_NAME_WIDTH display columns
---@param name string
---@param pad boolean pad with trailing spaces to a fixed width
---@return string
local function fit_conn_name(name, pad)
	local display_len = vim.fn.strdisplaywidth(name)

	if display_len <= CONN_NAME_WIDTH then
		return pad and (name .. string.rep(" ", CONN_NAME_WIDTH - display_len)) or name
	end

	local truncated, len = "", 0

	for i = 0, vim.fn.strchars(name) - 1 do
		local char = vim.fn.strcharpart(name, i, 1)
		local char_width = vim.fn.strdisplaywidth(char)

		if len + char_width + 1 > CONN_NAME_WIDTH then
			break
		end

		truncated = truncated .. char
		len = len + char_width
	end

	return pad and (truncated .. "…" .. string.rep(" ", CONN_NAME_WIDTH - len - 1)) or (truncated .. "…")
end

--- Get treesitter highlights for short format (verb + target): parses the
--- original query and returns the highlight group of the first keyword and
--- the first identifier/table name found after it.
---@param query string Original SQL query
---@return string verb_hl Highlight group for verb
---@return string target_hl Highlight group for target
local function get_short_highlights(query)
	local verb_hl = "@keyword.sql"
	local target_hl = "@variable.sql"

	local root, hl_query = parse_sql_highlights(query)

	if not root then
		return verb_hl, target_hl
	end

	local found_keyword, found_identifier = false, false

	for id, _ in hl_query:iter_captures(root, query, 0, -1) do
		local name = hl_query.captures[id]

		if not found_keyword and name:match("keyword") then
			verb_hl = "@" .. name .. ".sql"
			found_keyword = true
		end

		if
			found_keyword
			and not found_identifier
			and (name:match("variable") or name:match("identifier") or name:match("type"))
		then
			target_hl = "@" .. name .. ".sql"
			found_identifier = true
		end

		if found_keyword and found_identifier then
			break
		end
	end

	return verb_hl, target_hl
end

--- Render entries in compact mode (one line per entry)
---@param entries Dbab.HistoryEntry[]
---@param win_width number
---@param cfg table
---@return string[] lines, table[] highlights, table[] entry_line_map
local function render_compact(entries, win_width, cfg)
	local r_lines = {}
	local r_highlights = {}
	local r_line_map = {}

	--- Get query hints based on config
	---@param query string
	---@return string hints text
	---@return table[] hint_positions {hint, symbol_start, symbol_end, value_start, value_end}
	--- Append " <symbol>[ <value>]" to `result` and record its byte range in `positions`.
	---@param result string
	---@param positions table[]
	---@param hint_name string
	---@param symbol string
	---@param value? string
	---@return string
	local function append_hint(result, positions, hint_name, symbol, value)
		local symbol_start = #result + 1
		result = result .. " " .. symbol
		local pos = { hint = hint_name, symbol_start = symbol_start, symbol_end = #result }

		if value then
			pos.value_start = #result + 1
			result = result .. " " .. value
			pos.value_end = #result
		end

		table.insert(positions, pos)
		return result
	end

	--- Match "<keyword> ident[.ident]" case-insensitively, preferring the part after a dot
	--- (alias.column -> column).
	---@param query string
	---@param keyword string
	---@return string|nil
	local function match_column_after(query, keyword)
		local lower = keyword:lower()
		return query:match("%s" .. keyword .. "%s+[%w_]+%.([%w_]+)")
			or query:match("%s" .. lower .. "%s+[%w_]+%.([%w_]+)")
			or query:match("%s" .. keyword .. "%s+([%w_]+)")
			or query:match("%s" .. lower .. "%s+([%w_]+)")
	end

	local function get_query_hints(query)
		local hints = cfg.history.short_hints or {}
		local hint_set = {}

		for _, h in ipairs(hints) do
			hint_set[h] = true
		end

		local result = ""
		local positions = {}
		local upper_query = query:upper()

		if hint_set["where"] and upper_query:match("%sWHERE%s") then
			result = append_hint(result, positions, "where", "?", match_column_after(query, "WHERE"))
		end

		if hint_set["join"] and upper_query:match("%sJOIN%s") then
			local join_table = query:match("%sJOIN%s+([%w_]+)") or query:match("%sjoin%s+([%w_]+)")
			result = append_hint(result, positions, "join", "⋈", join_table)
		end

		if hint_set["order"] then
			local order_col = match_column_after(query, "ORDER%s+BY")

			if order_col then
				local direction = upper_query:match("%sORDER%s+BY%s+[%w_.]+%s+(DESC)") and "↓" or "↑"
				result = append_hint(result, positions, "order", direction, order_col)
			end
		end

		if hint_set["group"] then
			local group_col = match_column_after(query, "GROUP%s+BY")

			if group_col then
				result = append_hint(result, positions, "group", "⊞", group_col)
			end
		end

		if hint_set["limit"] then
			local limit_num = upper_query:match("%sLIMIT%s+(%d+)")

			if limit_num then
				result = append_hint(result, positions, "limit", "↓" .. limit_num)
			end
		end

		return result, positions
	end

	-- Determine format to use
	local format = cfg.history.format

	if not format then
		if cfg.history.filter_by_connection then
			format = { "time", "query", "duration" }
		else
			format = { "icon", "dbname", "time", "query", "duration" }
		end
	end

	-- Check which fields are in format
	local has_field = {}

	for _, field in ipairs(format) do
		has_field[field] = true
	end

	for i, entry in ipairs(entries) do
		local _, verb = history.format_summary(entry)
		local icon = history.get_verb_icon(verb)
		local time_str = os.date("%H:%M", entry.timestamp)
		local target = history.get_query_target(entry)
		local duration = history.format_duration(entry.duration_ms)
		local conn_icon = icons.db_default

		-- Build line and track highlight positions
		local line = ""
		local field_positions = {} -- {field, start_byte, end_byte}

		for _, field in ipairs(format) do
			local start_pos = #line

			if field == "icon" then
				-- Use DB icon if dbname is in format, otherwise verb icon
				if has_field.dbname then
					line = line .. "[" .. conn_icon .. " "
				else
					line = line .. icon
				end

				table.insert(field_positions, { field = "icon", verb = verb, start = start_pos, finish = #line })
			elseif field == "dbname" and entry.conn_name then
				local fitted_name = fit_conn_name(entry.conn_name, true)
				line = line .. fitted_name .. "] "
				table.insert(field_positions, { field = "dbname", start = start_pos, finish = start_pos + #fitted_name })
			elseif field == "time" then
				line = line .. time_str .. " "
				table.insert(field_positions, { field = "time", start = start_pos, finish = start_pos + #time_str })
			elseif field == "query" then
				local query_text
				local use_full = false
				local available_width = win_width - vim.fn.strdisplaywidth(line) - 15 -- reserve for duration

				-- Determine display mode
				local display_mode = cfg.history.query_display

				if display_mode == "auto" then
					-- Auto: use full if query fits, otherwise short
					local full_query = entry.query:gsub("%s+", " "):gsub("^%s+", ""):gsub("%s+$", "")

					if vim.fn.strdisplaywidth(full_query) <= available_width then
						display_mode = "full"
					else
						display_mode = "short"
					end
				end

				local hints_text = ""
				local hint_positions = {}

				if display_mode == "full" then
					-- Full query: normalize whitespace and truncate to fit
					query_text = entry.query:gsub("%s+", " "):gsub("^%s+", ""):gsub("%s+$", "")

					if vim.fn.strdisplaywidth(query_text) > available_width then
						-- Truncate with ellipsis
						local truncated = ""
						local len = 0

						for char_idx = 0, vim.fn.strchars(query_text) - 1 do
							local char = vim.fn.strcharpart(query_text, char_idx, 1)
							local char_width = vim.fn.strdisplaywidth(char)

							if len + char_width + 1 > available_width then
								break
							end

							truncated = truncated .. char
							len = len + char_width
						end

						query_text = truncated .. "…"
					end

					use_full = true
				else
					-- Short: verb + target (e.g., "SEL users") + hints
					query_text = verb .. " " .. target
					hints_text, hint_positions = get_query_hints(entry.query)
				end

				line = line .. query_text .. hints_text

				table.insert(field_positions, {
					field = "query",
					verb = verb,
					target = target,
					start = start_pos,
					query_end = start_pos + #query_text,
					finish = #line,
					full_query = entry.query,
					is_full = use_full,
					hints = hint_positions,
					hints_offset = start_pos + #query_text,
				})
			elseif field == "duration" and duration ~= "" then
				-- Duration is right-aligned, add padding
				local padding = math.max(1, win_width - vim.fn.strdisplaywidth(line) - #duration - 1)
				line = line .. string.rep(" ", padding) .. duration
				table.insert(field_positions, { field = "duration", start = #line - #duration, finish = #line })
			end
		end

		table.insert(r_lines, line)

		-- Compact: each entry is exactly one line
		r_line_map[i] = { start = i, finish = i }

		-- Highlights for this line (0-indexed, no header in buffer)
		local line_idx = i - 1

		-- Zebra striping background
		local row_hl = (i % 2 == 1) and "DbabHistoryRowOdd" or "DbabHistoryRowEven"
		table.insert(r_highlights, { line = line_idx, hl = row_hl, col_start = 0, col_end = -1 })

		-- Apply field-specific highlights
		for _, pos in ipairs(field_positions) do
			if pos.field == "icon" then
				local hl = has_field.dbname and "DbabSidebarIconConnection" or "DbabHistoryVerb"
				table.insert(r_highlights, { line = line_idx, hl = hl, col_start = pos.start, col_end = pos.finish })
			elseif pos.field == "dbname" then
				table.insert(
					r_highlights,
					{ line = line_idx, hl = "DbabHistoryConnName", col_start = pos.start, col_end = pos.finish }
				)
			elseif pos.field == "time" then
				table.insert(
					r_highlights,
					{ line = line_idx, hl = "DbabHistoryTime", col_start = pos.start, col_end = pos.finish }
				)
			elseif pos.field == "query" then
				if pos.is_full then
					-- Full mode: direct treesitter highlighting
					table.insert(r_highlights, {
						line = line_idx,
						hl = "treesitter_query",
						start_line = line_idx,
						first_line_offset = pos.start,
						other_line_offset = pos.start,
						query_text = r_lines[line_idx + 1]:sub(pos.start + 1, pos.query_end),
					})
				else
					-- Short mode: parse original query and map highlights to short format
					local verb_hl, target_hl = get_short_highlights(pos.full_query)
					local verb_end = pos.start + #pos.verb

					-- Verb highlight
					table.insert(r_highlights, {
						line = line_idx,
						hl = verb_hl,
						col_start = pos.start,
						col_end = verb_end,
					})

					-- Target highlight
					if pos.target and #pos.target > 0 then
						table.insert(r_highlights, {
							line = line_idx,
							hl = target_hl,
							col_start = verb_end + 1,
							col_end = pos.query_end,
						})
					end

					-- Hint highlights: symbol uses @keyword.sql, value uses @variable.member.sql
					if pos.hints and #pos.hints > 0 then
						for _, hint in ipairs(pos.hints) do
							-- Symbol highlight (?, ⋈, ↑, ↓, ⊞) with @keyword.sql
							table.insert(r_highlights, {
								line = line_idx,
								hl = "@keyword.sql",
								col_start = pos.hints_offset + hint.symbol_start,
								col_end = pos.hints_offset + hint.symbol_end,
							})

							-- Value highlight (column/table name) with @variable.member.sql
							if hint.value_start and hint.value_end then
								table.insert(r_highlights, {
									line = line_idx,
									hl = "@variable.member.sql",
									col_start = pos.hints_offset + hint.value_start,
									col_end = pos.hints_offset + hint.value_end,
								})
							end
						end
					end
				end
			elseif pos.field == "duration" then
				table.insert(
					r_highlights,
					{ line = line_idx, hl = "DbabHistoryDuration", col_start = pos.start, col_end = pos.finish }
				)
			end
		end
	end

	return r_lines, r_highlights, r_line_map
end

--- Render entries in detailed mode (multi-line with full query)
---@param entries Dbab.HistoryEntry[]
---@param win_width number
---@param cfg table
---@return string[] lines, table[] highlights, table[] entry_line_map
local function render_detailed(entries, win_width, cfg)
	local r_lines = {}
	local r_highlights = {}
	local r_line_map = {}
	local sep = " · "

	for i, entry in ipairs(entries) do
		local entry_start = #r_lines + 1

		local _, verb = history.format_summary(entry)
		local verb_icon = history.get_verb_icon(verb)

		local query_lines = vim.split(entry.query, "\n")
		local query_start_line = #r_lines

		for qi, qline in ipairs(query_lines) do
			if qi == 1 then
				table.insert(r_lines, verb_icon .. qline)
			else
				table.insert(r_lines, qline)
			end
		end

		table.insert(r_highlights, {
			line = query_start_line,
			hl = "treesitter_multiline",
			query_text = entry.query,
			start_line = query_start_line,
			first_line_offset = #verb_icon,
			other_line_offset = 0,
		})

		table.insert(r_highlights, { line = query_start_line, hl = "DbabHistoryVerb", col_start = 0, col_end = #verb_icon })

		local time_str = os.date("%H:%M", entry.timestamp)
		local duration = history.format_duration(entry.duration_ms)
		local row_count = entry.row_count
		local show_conn = not cfg.history.filter_by_connection

		local meta_parts = {}

		if show_conn then
			local conn_icon = icons.db_default
			local fitted_name = fit_conn_name(entry.conn_name or "unknown", false)
			table.insert(meta_parts, { text = conn_icon .. " " .. fitted_name, hl = "DbabHistoryConnName" })
		end

		table.insert(meta_parts, { text = time_str, hl = "DbabHistoryTime" })

		if row_count and row_count > 0 then
			local row_word = row_count == 1 and "row" or "rows"
			table.insert(meta_parts, { text = "󰓫 " .. row_count .. " " .. row_word, hl = "DbabHistoryDuration" })
		end

		if duration ~= "" then
			table.insert(meta_parts, { text = duration, hl = "DbabHistoryDuration" })
		end

		local meta_line = "  "
		local meta_highlights = {}

		for j, part in ipairs(meta_parts) do
			if j > 1 then
				local sep_start = #meta_line
				meta_line = meta_line .. sep
				table.insert(meta_highlights, { start = sep_start, finish = #meta_line, hl = "NonText" })
			end

			local part_start = #meta_line
			meta_line = meta_line .. part.text
			table.insert(meta_highlights, { start = part_start, finish = #meta_line, hl = part.hl })
		end

		table.insert(r_lines, meta_line)
		local meta_line_idx = #r_lines - 1

		table.insert(r_highlights, { line = meta_line_idx, hl = "NonText", col_start = 0, col_end = -1 })

		for _, mh in ipairs(meta_highlights) do
			table.insert(r_highlights, { line = meta_line_idx, hl = mh.hl, col_start = mh.start, col_end = mh.finish })
		end

		local entry_finish = #r_lines
		r_line_map[i] = { start = entry_start, finish = entry_finish }

		if i < #entries then
			local sep_line = string.rep("┄", win_width)
			table.insert(r_lines, sep_line)
			table.insert(r_highlights, { line = #r_lines - 1, hl = "WinSeparator", col_start = 0, col_end = -1 })
		end
	end

	return r_lines, r_highlights, r_line_map
end

--- Get entries currently visible in the panel (respects filter_by_connection)
---@return Dbab.HistoryEntry[]
local function get_filtered_entries()
	local cfg = config.get()
	local all_entries = history.get_all()

	if not cfg.history.filter_by_connection then
		return all_entries
	end

	local current_conn = get_current_connection_name()

	if not current_conn then
		return {}
	end

	local filtered = {}

	for _, entry in ipairs(all_entries) do
		if entry.conn_name == current_conn then
			table.insert(filtered, entry)
		end
	end

	return filtered
end

function M.render()
	if not M.buf or not vim.api.nvim_buf_is_valid(M.buf) then
		return
	end

	local cfg = config.get()
	local entries = get_filtered_entries()
	local lines = {}
	local highlights = {}

	local winbar_text = "%#DbabHistoryHeader#" .. icons.history .. " " .. "History%*"

	if cfg.history.filter_by_connection then
		local current_conn = get_current_connection_name()

		if current_conn then
			local conn_icon = icons.db_default .. " "
			winbar_text = "%#DbabHistoryHeader#"
				.. icons.history
				.. " "
				.. "History %#NonText#[%#DbabSidebarIconConnection#"
				.. conn_icon
				.. "%#Normal#"
				.. current_conn
				.. "%#NonText#]%*"
		end
	end
	-- Set winbar
	if M.win and vim.api.nvim_win_is_valid(M.win) then
		vim.api.nvim_win_set_option(M.win, "winbar", winbar_text)
	end

	-- History entries
	local win_width = 30

	if M.win and vim.api.nvim_win_is_valid(M.win) then
		win_width = vim.api.nvim_win_get_width(M.win)
	end

	if #entries == 0 then
		local empty_msg = "  No history yet"

		if cfg.history.filter_by_connection and not get_current_connection_name() then
			empty_msg = "  Connect to DB first"
		end

		table.insert(lines, empty_msg)
		table.insert(highlights, { line = 0, hl = "Comment", col_start = 0, col_end = -1 })
	else
		local style = cfg.history.style or "compact"
		local render_lines, render_highlights, line_map

		if style == "detailed" then
			render_lines, render_highlights, line_map = render_detailed(entries, win_width, cfg)
		else
			render_lines, render_highlights, line_map = render_compact(entries, win_width, cfg)
		end

		M.entry_line_map = line_map

		for _, l in ipairs(render_lines) do
			table.insert(lines, l)
		end

		for _, h in ipairs(render_highlights) do
			table.insert(highlights, h)
		end
	end

	-- Update buffer
	vim.api.nvim_buf_set_option(M.buf, "modifiable", true)
	vim.api.nvim_buf_set_lines(M.buf, 0, -1, false, lines)
	vim.api.nvim_buf_set_option(M.buf, "modifiable", false)

	-- Apply highlights
	local ns = vim.api.nvim_create_namespace("dbab_history")
	vim.api.nvim_buf_clear_namespace(M.buf, ns, 0, -1)

	for _, hl in ipairs(highlights) do
		if hl.query_text then
			apply_treesitter_highlights(
				M.buf,
				ns,
				hl.start_line,
				hl.first_line_offset or 0,
				hl.other_line_offset or 0,
				hl.query_text
			)
		else
			pcall(vim.api.nvim_buf_add_highlight, M.buf, ns, hl.hl, hl.line, hl.col_start, hl.col_end)
		end
	end
end

---@return Dbab.HistoryEntry|nil, number|nil
function M.get_entry_at_cursor()
	if not M.win or not vim.api.nvim_win_is_valid(M.win) then
		return nil, nil
	end

	local cursor = vim.api.nvim_win_get_cursor(M.win)
	local line = cursor[1] -- 1-indexed

	local entries = get_filtered_entries()

	-- Use entry_line_map to find which entry the cursor is on
	local entry_idx = nil

	if #M.entry_line_map > 0 then
		for i, range in ipairs(M.entry_line_map) do
			if line >= range.start and line <= range.finish then
				entry_idx = i
				break
			end
		end
	else
		-- Fallback for when line map is not yet populated (compact default)
		entry_idx = line
	end

	if entry_idx and entry_idx >= 1 and entry_idx <= #entries then
		return entries[entry_idx], entry_idx
	end

	return nil, nil
end

---@param buf number
function M.setup_keymaps(buf)
	local opts = { buffer = buf, noremap = true, silent = true }
	local keymaps = config.get().keymaps.history

	-- Enter: load or execute
	vim.keymap.set("n", keymaps.select, function()
		M.on_select()
	end, opts)

	-- Re-execute
	vim.keymap.set("n", keymaps.execute, function()
		M.execute_entry()
	end, opts)

	-- Copy query
	vim.keymap.set("n", keymaps.copy, function()
		local entry = M.get_entry_at_cursor()

		if entry then
			vim.fn.setreg("+", entry.query)
			vim.fn.setreg('"', entry.query)
			vim.notify("[dbab] Query copied", vim.log.levels.INFO)
		end
	end, opts)

	-- Delete entry
	vim.keymap.set("n", keymaps.delete, function()
		local entry, idx = M.get_entry_at_cursor()

		if entry and idx then
			vim.ui.select({ "Yes", "No" }, {
				prompt = "Delete this history entry?",
			}, function(choice)
				if choice == "Yes" then
					history.delete(idx)
					M.render()
				end
			end)
		end
	end, opts)

	-- Clear history for current connection
	vim.keymap.set("n", keymaps.clear, function()
		local conn = get_current_connection_name()

		if not conn then
			vim.notify("[dbab] No active connection", vim.log.levels.WARN)
			return
		end

		vim.ui.select({ "Yes", "No" }, {
			prompt = "Clear history for " .. conn .. "?",
		}, function(choice)
			if choice == "Yes" then
				history.clear_for_connection(conn)
				M.render()
				vim.notify("[dbab] History cleared for " .. conn, vim.log.levels.INFO)
			end
		end)
	end, opts)

	-- Close
	vim.keymap.set("n", config.get().keymaps.close, function()
		local workbench = require("dbab.ui.workbench")
		workbench.close()
	end, opts)

	-- S-Tab: To Result
	vim.keymap.set("n", keymaps.to_result, function()
		local workbench = require("dbab.ui.workbench")

		if workbench.result_win and vim.api.nvim_win_is_valid(workbench.result_win) then
			vim.api.nvim_set_current_win(workbench.result_win)
		end
	end, opts)

	-- Global keymaps (history picker, toggle history)
	require("dbab.ui.workbench").setup_global_buf_keymaps(buf)
end

--- Handle entry selection (load or execute based on config)
function M.on_select()
	local entry = M.get_entry_at_cursor()
	if not entry then
		return
	end

	local cfg = config.get()

	if cfg.history.on_select == "execute" then
		M.execute_entry()
	else
		M.load_entry()
	end
end

function M.load_entry()
	local entry = M.get_entry_at_cursor()

	if not entry then
		return
	end

	local workbench = require("dbab.ui.workbench")
	workbench.open_editor_with_query(entry.query)
end

function M.execute_entry()
	local entry = M.get_entry_at_cursor()

	if not entry then
		return
	end

	require("dbab.ui.workbench").run_history_entry(entry)
end

---@param win number
function M.setup(win)
	M.win = win
	local buf = M.get_or_create_buf()
	vim.api.nvim_win_set_buf(win, buf)

	-- Window options (must set AFTER buffer is attached to window)
	vim.api.nvim_win_set_option(win, "number", false)
	vim.api.nvim_win_set_option(win, "relativenumber", false)
	vim.api.nvim_win_set_option(win, "signcolumn", "no")
	vim.api.nvim_win_set_option(win, "foldcolumn", "0")
	vim.api.nvim_win_set_option(win, "cursorline", true)
	vim.api.nvim_win_set_option(win, "wrap", false)
	vim.api.nvim_win_set_option(win, "winfixwidth", true)

	-- Setup keymaps
	M.setup_keymaps(buf)

	-- Load and render
	history.load()
	M.render()
end

function M.cleanup()
	if M.buf and vim.api.nvim_buf_is_valid(M.buf) then
		pcall(vim.api.nvim_buf_delete, M.buf, { force = true })
	end

	M.buf = nil
	M.win = nil
	M.entry_line_map = {}
end

return M
