--- See lua/dbab/types.lua for type definitions (Dbab.QueryResult)

local M = {}

--- Extract the substring of `line` spanning display columns [from, to] (1-based, inclusive).
--- Column boundaries come from psql's ASCII separator line, where byte position equals
--- display column. Data lines may contain UTF-8, where byte offset != display column, so
--- pure byte slicing corrupts multibyte cells. ASCII lines take a fast path (no behavior change).
---@param line string
---@param from number
---@param to number
---@return string
local function slice_display(line, from, to)
	if not line:find("[\128-\255]") then
		if from > #line then
			return ""
		end
		return line:sub(from, math.min(to, #line))
	end

	local out = {}
	local col = 1
	local i = 1
	local n = #line
	while i <= n do
		local b = line:byte(i)
		local len = 1
		if b >= 0xF0 then
			len = 4
		elseif b >= 0xE0 then
			len = 3
		elseif b >= 0xC0 then
			len = 2
		end
		local ch = line:sub(i, i + len - 1)
		local w = vim.fn.strdisplaywidth(ch)
		if w == 0 then
			w = 1
		end
		-- char occupies display cols [col, col+w-1]
		if col + w - 1 >= from and col <= to then
			table.insert(out, ch)
		end
		col = col + w
		i = i + len
		if col > to then
			break
		end
	end
	return table.concat(out)
end

---@param raw string Raw output from database
---@param style? Dbab.ResultStyle "table" (default), "json", "raw", "vertical", "markdown"
---@return Dbab.QueryResult
function M.parse(raw, style)
	local result = {
		columns = {},
		rows = {},
		row_count = 0,
		raw = raw,
	}

	if style == "raw" then
		result.columns = { "raw" }
		for _, line in ipairs(vim.split(raw, "\n")) do
			table.insert(result.rows, { line })
		end
		result.row_count = #result.rows
		return result
	end

	local table_result = M.parse_table(raw)

	if style == "json" then
		local json_data = {}
		for _, row in ipairs(table_result.rows) do
			local item = {}
			for i, col in ipairs(table_result.columns) do
				item[col] = row[i] or ""
			end
			table.insert(json_data, item)
		end

		-- Encode each item first, then join with commas, so a skipped (failed) item
		-- never leaves a dangling comma that would produce invalid JSON.
		local encoded = {}
		for _, item in ipairs(json_data) do
			local ok, item_str = pcall(vim.json.encode, item)
			if ok then
				table.insert(encoded, item_str)
			end
		end

		local lines = { "[" }
		for idx, item_str in ipairs(encoded) do
			local suffix = idx < #encoded and "," or ""
			table.insert(lines, "  " .. item_str .. suffix)
		end
		table.insert(lines, "]")

		local pretty = table.concat(lines, "\n")
		result.raw = pretty
		result.columns = table_result.columns
		for _, line in ipairs(lines) do
			table.insert(result.rows, { line })
		end
		result.row_count = #encoded
		return result
	end

	if style == "vertical" then
		local col_width = 0
		for _, col in ipairs(table_result.columns) do
			col_width = math.max(col_width, #col)
		end

		local lines = {}
		for idx, row in ipairs(table_result.rows) do
			table.insert(lines, string.format("-[ RECORD %d ]%s", idx, string.rep("-", 16)))
			for i, col in ipairs(table_result.columns) do
				local padded = col .. string.rep(" ", col_width - #col)
				table.insert(lines, string.format("%s | %s", padded, row[i] or ""))
			end
		end

		result.raw = table.concat(lines, "\n")
		result.columns = table_result.columns
		for _, line in ipairs(lines) do
			table.insert(result.rows, { line })
		end
		result.row_count = #table_result.rows
		return result
	end

	if style == "markdown" then
		local widths = M.calculate_column_widths(table_result)

		local lines = {}
		local header_parts = {}
		for i, col in ipairs(table_result.columns) do
			table.insert(header_parts, " " .. col .. string.rep(" ", widths[i] - #col) .. " ")
		end
		table.insert(lines, "|" .. table.concat(header_parts, "|") .. "|")

		local sep_parts = {}
		for _, w in ipairs(widths) do
			table.insert(sep_parts, string.rep("-", w + 2))
		end
		table.insert(lines, "|" .. table.concat(sep_parts, "|") .. "|")

		for _, row in ipairs(table_result.rows) do
			local row_parts = {}
			for i, cell in ipairs(row) do
				local w = widths[i] or #cell
				table.insert(row_parts, " " .. cell .. string.rep(" ", w - #cell) .. " ")
			end
			table.insert(lines, "|" .. table.concat(row_parts, "|") .. "|")
		end

		result.raw = table.concat(lines, "\n")
		result.columns = table_result.columns
		for _, line in ipairs(lines) do
			table.insert(result.rows, { line })
		end
		result.row_count = #table_result.rows
		return result
	end

	return table_result
end

---@param raw string
---@return Dbab.QueryResult
function M.parse_table(raw)
	local lines = vim.split(raw, "\n")

	lines = vim.tbl_filter(function(line)
		if line:match("^mysql: %[Warning%]") then
			return false
		end
		-- Strip mongosh noise: prompt remnants and pagination hint
		if line:match("^%w+> ") then
			return false
		end
		if line:match('^Type "it" for more') then
			return false
		end
		if line == "" then
			return true
		end
		return true
	end, lines)

	local result = {
		columns = {},
		rows = {},
		row_count = 0,
		raw = raw,
	}

	if #lines == 0 then
		result.columns = { "result" }
		return result
	end

	local header_line = lines[1]
	local separator_line = lines[2] or ""

	if header_line:find("\t") then
		result.columns = vim.split(header_line, "\t")
		for i = 2, #lines do
			local line = lines[i]
			if line ~= "" then
				local row = vim.split(line, "\t")
				table.insert(result.rows, row)
			end
		end
		result.row_count = #result.rows
		return result
	end

	if not separator_line:match("^%-") and not separator_line:match("^%+") then
		result.columns = { "result" }
		for _, line in ipairs(lines) do
			if line ~= "" then
				table.insert(result.rows, { line })
			end
		end
		result.row_count = #result.rows
		return result
	end

	local col_positions = {}
	local pos = 1
	for segment in separator_line:gmatch("[%-]+") do
		local start_pos = separator_line:find(segment, pos, true)
		local end_pos = start_pos + #segment - 1
		table.insert(col_positions, { start = start_pos, finish = end_pos })
		pos = end_pos + 1
	end

	for _, col_pos in ipairs(col_positions) do
		local col_name = header_line:sub(col_pos.start, col_pos.finish)
		col_name = vim.trim(col_name)
		table.insert(result.columns, col_name)
	end

	for i = 3, #lines do
		local line = lines[i]

		if line:match("^%(%d+ rows?%)") then
			local count = line:match("%((%d+) rows?%)")
			result.row_count = tonumber(count) or #result.rows
			break
		end

		if line ~= "" then
			local row = {}
			for _, col_pos in ipairs(col_positions) do
				local cell = vim.trim(slice_display(line, col_pos.start, col_pos.finish))
				table.insert(row, cell)
			end
			table.insert(result.rows, row)
		end
	end

	if result.row_count == 0 then
		result.row_count = #result.rows
	end

	return result
end

---@param result Dbab.QueryResult
---@return number[] Column widths
function M.calculate_column_widths(result)
	local widths = {}

	for i, col in ipairs(result.columns) do
		widths[i] = #col
	end

	for _, row in ipairs(result.rows) do
		for i, cell in ipairs(row) do
			widths[i] = math.max(widths[i] or 0, #cell)
		end
	end

	return widths
end

return M
