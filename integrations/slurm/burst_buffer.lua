-- Copyright 2026. Triad National Security, LLC. All rights reserved.

-- ====== CONFIG ======
local CONDUIT_CLI = "/usr/sbin/conduit"
local CONDUIT_CERT = "/etc/conduit/conduit-slurm-cert.pem"
local CONDUIT_KEY = "/etc/conduit/conduit-slurm-key.pem"
local CONDUIT_CA = "/etc/conduit/conduit-external-ca.pem"
local CONDUIT_CLI_CONFIG = "/etc/conduit/conduit-cli-config.yaml"


local STATIC_FLAGS = { "--cert", CONDUIT_CERT, "--key", CONDUIT_KEY, "--ca", CONDUIT_CA, "--config", CONDUIT_CLI_CONFIG }


DIRECTIVE           = "CONDUIT"
COMMENT_JOB         = "SLURMJOB:"
COMMENT_INDEX       = "SLURMINDEX:"
COMMENT_TYPE        = "SLURMTYPE:"

-- conduit types
CONDUIT_PRE         = DIRECTIVE .. "_PRE"
CONDUIT_POST        = DIRECTIVE .. "_POST"

-- =====================

lua_script_name     = "burst_buffer.lua"

local posix         = require("posix")

CONDUIT_JOB         = {
	jobID = "",   -- the slurm job id
	jobType = "", -- the type of job (either conduit_in or conduit_out)
	jobIndex = "", -- the index of the conduit directive in the slurm job script
	transferID = "", -- the conduit transfer ID
	userArgs = {},
	uid = 0,
	workDir = "",
}
CONDUIT_JOB.__index = CONDUIT_JOB

function CONDUIT_JOB:new(jobID, jobType, jobIndex, userArgs, uid, workDir)
	local job = {}
	setmetatable(job, self)
	self.__index = self
	job.jobID = jobID    -- slurm jobid
	job.jobType = jobType -- CONDUIT_PRE or CONDUIT_POST
	job.jobIndex = jobIndex -- the index of the job in the sbatch file
	job.uid = uid        -- user id
	job.userArgs = userArgs -- provided transfer command
	job.transferID = ""
	job.workDir = workDir
	return job
end

function CONDUIT_JOB:comment()
	return COMMENT_JOB .. self.jobID .. "," .. COMMENT_INDEX .. self.jobIndex .. "," .. COMMENT_TYPE .. self.jobType
end

function CONDUIT_JOB:validationCmd()
	local cmd, final_args = self:transferCmd()

	table.insert(final_args, "--validate-only")

	return cmd, final_args
end

function CONDUIT_JOB:describeJsonPathCmd(jsonpath)
	local final_args = {}

	table.insert(final_args, "describe")
	table.insert(final_args, self.transferID)

	table.insert(final_args, "--quiet")

	table.insert(final_args, "--jsonpath")
	table.insert(final_args, jsonpath)

	table.insert(final_args, "--user")
	table.insert(final_args, tostring(self.uid))

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CLI, final_args
end

function CONDUIT_JOB:stateCmd()
	local jsonpath = "x[0].state"
	return self:describeJsonPathCmd(jsonpath)
end

function CONDUIT_JOB:activeCmd()
	local jsonpath = "x[0].active"
	return self:describeJsonPathCmd(jsonpath)
end

function CONDUIT_JOB:transferIDCmd()
	local jsonpath = "x[0].transferID"

	local final_args = {}

	table.insert(final_args, "describe")
	table.insert(final_args, "--quiet")

	table.insert(final_args, self:comment())

	table.insert(final_args, "--jsonpath")
	table.insert(final_args, jsonpath)

	table.insert(final_args, "--user")
	table.insert(final_args, tostring(self.uid))

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CLI, final_args
end

function CONDUIT_JOB:errorCmd()
	-- client:/go$ conduit describe 9a73362d-223e-49aa-ac9b-3dc0e0028546 -o json --jsonpath x[0].error
	-- "ERROR_LEASE_EXPIRED"
	local jsonpath = "x[0].error"
	return self:describeJsonPathCmd(jsonpath)
end

function CONDUIT_JOB:errorMessageCmd()
	-- client:/go$ conduit describe 9a73362d-223e-49aa-ac9b-3dc0e0028546 -o json --jsonpath x[0].errorMessage
	-- ""
	local jsonpath = "x[0].errorMessage"
	return self:describeJsonPathCmd(jsonpath)
end

function CONDUIT_JOB:transferCmd()
	local final_args = {}

	-- append self.userArgs to final_args
	for i = 1, #self.userArgs do
		final_args[#final_args + 1] = self.userArgs[i]
	end

	table.insert(final_args, "--quiet")
	table.insert(final_args, "--skip-stat")

	table.insert(final_args, "--user")
	table.insert(final_args, tostring(self.uid))

	table.insert(final_args, "--comment")
	table.insert(final_args, self:comment())

	if type(self.workDir) == "string" and self.workDir ~= "" then
		table.insert(final_args, "--work-dir")
		table.insert(final_args, self.workDir)
	end

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CLI, final_args
end

-- requires a transferID in CONDUIT_JOB
function CONDUIT_JOB:abortCmd()
	local final_args = {}

	table.insert(final_args, "abort")
	table.insert(final_args, self.transferID)

	table.insert(final_args, "--user")
	table.insert(final_args, tostring(self.uid))

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CLI, final_args
end

function CONDUIT_JOB:transferAndWatchCmd()
	local cmd, final_args = self:transferCmd()

	table.insert(final_args, "--watch")

	return cmd, final_args
end

function CONDUIT_JOB:watchCmd()
	local final_args = {}

	table.insert(final_args, "watch")
	table.insert(final_args, self.transferID)

	table.insert(final_args, "--user")
	table.insert(final_args, tostring(self.uid))

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CLI, final_args
end

local function Is_uuid(str)
	-- optional single or double quote at start and end
	return str:match([[^["']?%x%x%x%x%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x["']?$]]) ~= nil
end

local function Extract_uuid(str)
	if type(str) ~= "string" then
		return ""
	end

	return str:match([[%x%x%x%x%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x]]) or ""
end


function CONDUIT_JOB:getTransferIDFromConduit()
	local cmd, args = self.transferIDCmd(self)
	local done, output = exec_cmd(cmd, args)
	slurm.log_debug(string.format("cmd: %s %s", cmd, table.concat(args, " ")))
	if done == false then
		return "", "failed to get transferID from conduit with comment: " .. self:comment() .. " :command failed: " .. output
	end
	-- make sure we get back a transferID that's a string
	if type(output) ~= type("") then
		slurm.log_error(string.format("%s: getTransferIDFromConduit(), jobIndex=%s, transferID=%s : failed to get transfer ID from conduit", lua_script_name, self.jobIndex, tostring(output)))
		return "", "failed to get transferID from conduit with comment: " .. self:comment() .. " received invalid transferID: " .. tostring(output)
	end

	local transferID = string.gsub(output, "%s", "")
	transferID = string.gsub(transferID, "\"", "")

	if not Is_uuid(transferID) then
		slurm.log_error(string.format("%s: getTransferIDFromConduit(), jobIndex=%s, transferID=%s : failed to get transfer ID from conduit", lua_script_name, self.jobIndex, tostring(output)))
		return "", "failed to get transferID from conduit with comment: " .. self:comment() .. " received invalid transferID: " .. tostring(output)
	end

	return transferID, ""
end

local function SlurmJobCommentPrefix(slurmJobID)
	return COMMENT_JOB .. tostring(slurmJobID) .. ","
end

local BBSTAT_MAX_TRANSFERS = 100

local function SlurmIDStateCmd(slurmJobID, uid)
	local query = SlurmJobCommentPrefix(slurmJobID)

	local jsonpath = '$..["transferID","state","error"]'

	local final_args = {}

	table.insert(final_args, "describe")
	table.insert(final_args, "--quiet")

	table.insert(final_args, query)

	table.insert(final_args, "-n")
	table.insert(final_args, tostring(BBSTAT_MAX_TRANSFERS))

	table.insert(final_args, "--jsonpath")
	table.insert(final_args, jsonpath)

	table.insert(final_args, "--user")
	table.insert(final_args, tostring(uid))

	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CLI, final_args
end

function TransferExistsCmd(comment, uid)
	local final_args = {}

	table.insert(final_args, "describe")
	table.insert(final_args, "--quiet")

	table.insert(final_args, comment)

	table.insert(final_args, "--user")
	table.insert(final_args, tostring(uid))

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	local done, output = exec_cmd(CONDUIT_CLI, final_args)
	if done == false then
		return false, "failed to get transfer from conduit with comment: " .. comment .. " :command failed: " .. tostring(output)
	end
	-- make sure we get back an output that's a string
	if type(output) ~= type("") then
		return false, "failed to get transferID from conduit with comment: " .. comment .. " received invalid output: " .. tostring(output)
	end
	output = string.gsub(output, "%s", "")
	output = string.gsub(output, "\"", "")

	if output == "[]" then
		return false, ""
	end

	return true, ""
end

-- exec_cmd: run a command (no shell) and collect its stdout+stderr.
-- cmd: program name (e.g., "ls" or "/bin/ls")
-- args: table of arguments
function exec_cmd(cmd, args)
	if type(cmd) ~= "string" or cmd == "" then
		return false, "invalid cmd"
	end
	if type(args) ~= "table" or #args == 0 then
		return false, "invalid args"
	end

	-- pipe to capture child's stdout+stderr
	local r_fd, w_fd = posix.pipe()
	if not r_fd then
		return false, "failed to create pipe"
	end

	local pid, fork_err, fork_errno = posix.fork()
	if pid == nil then
		posix.close(r_fd)
		posix.close(w_fd)
		return false, "failed to fork: " .. tostring(fork_err or fork_errno)
	end

	if pid == 0 then
		-- child
		-- dup pipe write end onto STDOUT/STDERR
		posix.dup2(w_fd, posix.STDOUT_FILENO)
		posix.dup2(w_fd, posix.STDERR_FILENO)
		-- close fds we don't need in child
		posix.close(r_fd)
		posix.close(w_fd)

		-- execp: searches PATH if cmd is not absolute
		posix.execp(cmd, args)
		-- only reaches here on error
		posix._exit(127)
	end

	-- parent
	posix.close(w_fd)

	local chunks = {}
	while true do
		local chunk, rerr, errno = posix.read(r_fd, 4096)
		if chunk == nil then
			if errno == posix.EINTR then
				-- interrupted, retry
			else
				-- read error; break but still wait & return what we have
				break
			end
		elseif #chunk == 0 then
			-- EOF
			break
		else
			chunks[#chunks + 1] = chunk
		end
	end
	posix.close(r_fd)

	local _, reason, status = posix.wait(pid)
	local out = table.concat(chunks)
	if reason ~= "exited" or status ~= 0 then
		return false, out .. " " .. reason .. " " .. status
	end
	return true, out
end

local RESERVED_USER_FLAGS = {
	["--cert"] = true,
	["--key"] = true,
	["--ca"] = true,
	["-c"] = true,
	["--config"] = true,
	["--user"] = true,
	["--comment"] = true,
	["--work-dir"] = true,
	["--quiet"] = true,
	["-q"] = true,
	["--skip-stat"] = true,
	["--validate-only"] = true,
	["--watch"] = true,
	["--debug"] = true,
	["-d"] = true,
}

local function option_name(tok)
	return tok:match("^(%-%-[^=]+)=") or tok
end

local function validate_user_args(toks)
	for i = 1, #toks do
		local tok = toks[i]

		if tok == "--" then
			return false, "reserved option terminator '--' is not allowed in CONDUIT directives"
		end

		local name = option_name(tok)
		if RESERVED_USER_FLAGS[name] then
			return false, "reserved option " .. name .. " is not allowed in CONDUIT directives"
		end
	end

	return true, ""
end

local function tokenize(s)
	if type(s) ~= "string" then return nil, "input must be a string" end
	-- if has_control(s) then return nil, "control characters not allowed" end
	local t = {}
	for tok in s:gmatch("%S+") do t[#t + 1] = tok end
	if #t == 0 then return nil, "empty directive" end
	return t
end

local function parse_conduit_directives(job_script, jobID, uid, work_dir)
	local Conduit_directives = {}
	local idx = 1
	local bb
	-- local line

	io.input(job_script)
	local content = io.read("*all")

	local mline = "^#" .. DIRECTIVE

	-- split up script by line
	for line in content:gmatch("[^\n]+") do
		-- check if line matches the conduit directive
		bb = line:match(mline)
		if bb ~= nil then
			local conduit_pre = false
			local conduit_post = false
			-- local action = ""
			-- local source = ""
			-- local destination = ""
			local jobType = nil
			local cmd = ""

			-- go over each word of the line
			for w in line:gmatch("%S+") do
				if w == "#" .. CONDUIT_PRE then
					conduit_pre = true
				elseif w == "#" .. CONDUIT_POST then
					conduit_post = true
				end
			end

			-- check if directive line is malformed
			if (conduit_pre and conduit_post) or (not conduit_pre and not conduit_post) then
				slurm.log_error("%s: find_conduit_directives(): sbatch directive malformed. must contain either 'CONDUIT_PRE' and 'CONDUIT_POST'", lua_script_name, line)
				return nil, "directive must contain either CONDUIT_PRE or CONDUIT_POST"
			else
				if conduit_pre then
					jobType = CONDUIT_PRE
				elseif conduit_post then
					jobType = CONDUIT_POST
				end
			end

			if jobType ~= nil then
				local beg, final = string.find(line, jobType)
				cmd = string.sub(line, final + 1)
			end

			if cmd == "" then
				return nil, "failed to parse command from directive: " .. line
			end

			-- tokenize the line
			local toks, err = tokenize(cmd)
			if not toks then return nil, err end

			local ok
			ok, err = validate_user_args(toks)
			if not ok then return nil, err end

			local Job = CONDUIT_JOB:new(jobID, jobType, idx, toks, uid, work_dir)

			-- create conduit job
			Conduit_directives[idx] = Job
			idx = idx + 1
		end
	end

	return Conduit_directives
end

local function get_work_dir(job_info)
	if type(job_info) ~= "table" then
		return ""
	end

	local work_dir = job_info["work_dir"]
	if type(work_dir) == "string" and work_dir ~= "" then
		return work_dir
	end

	return ""
end

--[[
--slurm_bb_job_process
--
--WARNING: This function is called synchronously from slurmctld and must
--return quickly.
--
--This function is called on job submission.
--
--Send our job to conduit as a "validation" job. This will only run validation so we can verify permissions will work out
--]]
function slurm_bb_job_process(job_script, uid, gid, job_info)
	slurm.log_debug("conduit bb process")
	local contents
	local job_id = job_info["job_id"]
	local work_dir = get_work_dir(job_info)
	slurm.log_info("%s: slurm_bb_job_process(). job_script=%s, uid=%s, gid=%s, job_id=%s, work_dir=%s", lua_script_name, job_script, uid, gid, job_id, work_dir)
	io.input(job_script)
	contents = io.read("*all")

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid, work_dir)
	if err ~= nil or conduit_jobs == nil then
		slurm.log_error("failed to parse directives: " .. err)
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_PRE then
			local cmd, args = j:validationCmd()
			slurm.log_debug(table.concat(args, " "))
			local done, output = exec_cmd(cmd, args)

			if type(output) ~= "string" then
				slurm.log_debug(string.format("%s: slurm_bb_job_process(), jobIndex=%s, output=%s : failed to run validation command", lua_script_name, j.jobIndex, tostring(output)))
				return slurm.ERROR, "failed to run validation command for directive " .. j.jobIndex .. " received invalid output: " .. tostring(output)
			end

			local transferID = Extract_uuid(output)

			slurm.log_debug("sent validation to conduit: " .. output)

			-- done == false means the command failed
			if done == false then
				slurm.log_debug(string.format("%s: slurm_bb_job_process(), jobIndex=%s, output=%s : failed to run validation command", lua_script_name, j.jobIndex, tostring(output)))

				local response = "validation failed for directive " .. j.jobIndex .. ": " .. output

				if Is_uuid(transferID) then
					j.transferID = transferID

					local eCmd, eArgs = j:errorCmd()
					local emCmd, emArgs = j:errorMessageCmd()
					local errDone, err = exec_cmd(eCmd, eArgs)
					local errMessageDone, errMessage = exec_cmd(emCmd, emArgs)

					slurm.log_debug(string.format("%s: slurm_bb_job_process(), errDone=[%s], err=[%s]", lua_script_name, tostring(errDone), tostring(err)))
					slurm.log_debug(string.format("%s: slurm_bb_job_process(), errMessageDone=[%s], errMessage=[%s]", lua_script_name, tostring(errMessageDone), tostring(errMessage)))

					if errDone == true and errMessageDone == true then
						response = response .. ": " .. err .. " " .. errMessage
					end
				end

				response = response .. ": " .. table.concat(j.userArgs, " ")
				return slurm.ERROR, response
			end

			if not Is_uuid(transferID) then
				return slurm.ERROR, "failed to run validation command for directive " .. j.jobIndex .. " received invalid transferID: " .. tostring(output)
			end

			j.transferID = transferID
		end
	end

	return slurm.SUCCESS, contents
end

--[[
--slurm_bb_pools
--
--WARNING: This function is called from slurmctld and must return quickly.
--
--This function is called on slurmctld startup, and then periodically while
--slurmctld is running.
--
--You may specify "pools" of resources here. If you specify pools, a job may
--request a specific pool and the amount it wants from the pool. Slurm will
--subtract the job's usage from the pool at slurm_bb_data_in and Slurm will
--add the job's usage of those resources back to the pool after
--slurm_bb_teardown.
--A job may choose not to specify a pool even you pools are provided.
--If pools are not returned here, Slurm does not track burst buffer resources
--used by jobs.
--
--If pools are desired, they must be returned as the second return value
--of this function. It must be a single JSON string representing the pools.
--]]
function slurm_bb_pools()
	-- conduit does not use slurm_bb_pools
	return slurm.SUCCESS
end

-- Scancel will run the Slurm scancel command and collect its output.
-- On success this returns true and the output of the command.
-- On failure this returns false and the output of the command.
local function Scancel(jobId, hurry)
	local args = {}

	if hurry == true then
		args[#args + 1] = "--hurry"
	end

	args[#args + 1] = tostring(jobId)

	return exec_cmd("scancel", args)
end

--[[
--slurm_bb_job_teardown
--
--This function is called asynchronously and is not required to return quickly.
--This function is normally called after the job completes (or is cancelled).
--
-- Conduit aborts any running jobs during
--]]
function slurm_bb_job_teardown(job_id, job_script, hurry, uid, gid)
	slurm.log_info("slurm_bb_job_teardown(). job id:%s, job script:%s, hurry:%s, uid:%s, gid:%s", job_id, job_script, hurry, uid, gid)

	local hurry_flag = false
	if hurry == "true" then
		hurry_flag = true
	end

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid, nil)
	if err ~= nil or conduit_jobs == nil then
		return slurm.ERROR, "failed to parse directives: " .. err
	end


	-- local jobs_actives = {}

	-- get transferIDs from conduit and attach them to their respective job
	for i, j in pairs(conduit_jobs) do
		local exists = false
		exists, err = TransferExistsCmd(j:comment(), uid)
		if err ~= "" then
			return slurm.ERROR, "failed to check if directive exists in conduit " .. j.jobIndex .. " " .. err
		end

		if exists then
			slurm.log_debug(string.format("transfer for directive %s exists", j.jobIndex))
			local transferID, err = j:getTransferIDFromConduit()
			if err ~= "" then
				return slurm.ERROR, "failed to get transfer ID for directive " .. j.jobIndex .. " " .. err
			end
			conduit_jobs[i].transferID = transferID
		else
			slurm.log_debug(string.format("transfer for directive %s does not exist", j.jobIndex))
		end
	end

	-- if we got a trasnfer id for any jobs, abort them
	for i, j in pairs(conduit_jobs) do
		if j.transferID ~= "" then
			local cmd, args = j:activeCmd()
			local done, active = exec_cmd(cmd, args)
			slurm.log_debug(table.concat(args, " "))

			-- check if our describe command failed (this should really never happen unless there is a network issue)
			if done == false then
				slurm.log_error(string.format("%s: slurm_bb_job_teardown(), jobIndex=%s, transferID=%s : failed to get transfer active state", lua_script_name, j.jobIndex, j.transferID))

				local response = "failed to get active state for directive " .. j.jobIndex
				return slurm.ERROR, response
			end


			-- make sure we get back a state that's a string
			if type(active) ~= type("") then
				slurm.log_error(string.format("%s: slurm_bb_job_teardown(), jobIndex=%s, output=%s : failed to get transfer active state", lua_script_name, j.jobIndex, tostring(active)))
				return slurm.ERROR, "failed to run transfer active status command for directive " .. j.jobIndex .. " received invalid active state: " .. tostring(active)
			end
			active = string.gsub(active, "%s", "")
			active = string.gsub(active, "\"", "")

			if active == "true" then
				-- the job is still active
				if hurry_flag == true then
					-- we're in a hurry, attempt to abort the job
					local cmd, args = j:abortCmd()
					slurm.log_debug(table.concat(args, " "))
					local done, transferID = exec_cmd(cmd, args)
					if done == false then
						slurm.log_error(string.format("%s: slurm_bb_job_teardown(), jobIndex=%s, output=%s : failed to get transfer active state", lua_script_name, j.jobIndex, tostring(transferID)))

						local response = "failed to abort transfer for directive " .. j.jobIndex .. " transferid: " .. j.transferID
						return slurm.ERROR, response
					end
				else
					-- we're not in a hurry, wait for the transfer to complete
					local cmd, args = j:watchCmd()
					slurm.log_debug(table.concat(args, " "))
					local done, transferID = exec_cmd(cmd, args)
					if done == false then
						slurm.log_error(string.format("%s: slurm_bb_job_teardown(), jobIndex=%s, output=%s : failed to get transfer active state", lua_script_name, j.jobIndex, tostring(transferID)))

						local response = "failed to watch state for directive " .. j.jobIndex .. " transferid: " .. j.transferID
						return slurm.ERROR, response
					end
				end
			end
		end
	end

	slurm.log_info("all transfers teardown successful, sending best-effort scancel")
	local done, output = Scancel(job_id, hurry_flag)

	if done then
		slurm.log_debug(string.format("successfully sent scancel command for job %s: %s", job_id, tostring(output)))
	else
		slurm.log_info(string.format("best-effort scancel failed for job %s: %s", job_id, tostring(output)))
	end

	return slurm.SUCCESS, ""
end

--[[
--slurm_bb_setup
--
--This function is called asynchronously and is not required to return quickly.
--This function is called while the job is pending.
--]]
function slurm_bb_setup(job_id, uid, gid, pool, bb_size, job_script, job_info)
	slurm.log_info("slurm_bb_setup(). job id:%s, uid: %s, gid:%s, pool:%s, size:%s, job script:%s", job_id, uid, gid, pool, bb_size, job_script)

	return slurm.SUCCESS
end

--[[
--slurm_bb_data_in
--
--This function is called asynchronously and is not required to return quickly.
--This function is called immediately after slurm_bb_setup while the job is
--pending.
--]]
function slurm_bb_data_in(job_id, job_script, uid, gid, job_info)
	local work_dir = get_work_dir(job_info)

	slurm.log_info("slurm_bb_data_in(). job id:%s, job script:%s, uid:%s, gid:%s, work_dir:%s", job_id, job_script, uid, gid, work_dir)

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid, work_dir)
	if err ~= nil or conduit_jobs == nil then
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	slurm.log_debug("parsed conduit directives successfully")

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_PRE then
			local cmd, args = j:transferCmd()
			slurm.log_debug(table.concat(args, " "))
			local done, output = exec_cmd(cmd, args)
			-- make sure we get back a transferID that's a string
			if type(output) ~= "string" then
				slurm.log_debug(string.format("%s: slurm_bb_data_in(), jobIndex=%s, output=%s : failed to run transfer command", lua_script_name, j.jobIndex, tostring(output)))
				return slurm.ERROR, "failed to run transfer command for directive " .. j.jobIndex .. " received invalid output: " .. tostring(output)
			end

			local transferID = Extract_uuid(output)

			if done == false then
				slurm.log_debug(string.format("%s: slurm_bb_data_in(), jobIndex=%s, output=%s : failed to run transfer command", lua_script_name, j.jobIndex, tostring(output)))

				local response = "transfer failed for directive " .. j.jobIndex .. ": " .. output

				if Is_uuid(transferID) then
					j.transferID = transferID

					local eCmd, eArgs = j:errorCmd()
					local emCmd, emArgs = j:errorMessageCmd()
					local errDone, err = exec_cmd(eCmd, eArgs)
					local errMessageDone, errMessage = exec_cmd(emCmd, emArgs)

					slurm.log_debug(string.format("%s: slurm_bb_data_in(), errDone=[%s], err=[%s]", lua_script_name, tostring(errDone), tostring(err)))
					slurm.log_debug(string.format("%s: slurm_bb_data_in(), errMessageDone=[%s], errMessage=[%s]", lua_script_name, tostring(errMessageDone), tostring(errMessage)))

					if errDone == true and errMessageDone == true then
						response = response .. ": " .. err .. " " .. errMessage
					end
				end

				response = response .. ": " .. table.concat(j.userArgs, " ")
				return slurm.ERROR, response
			end

			if not Is_uuid(transferID) then
				return slurm.ERROR, "failed to run transfer command for directive " .. j.jobIndex .. " received invalid transferID: " .. tostring(output)
			end

			j.transferID = transferID
		end
	end

	return slurm.SUCCESS, ""
end

--[[
--slurm_bb_test_data_in
--
--This function is called asynchronously and is not required to return quickly.
--This function is called immediately after slurm_bb_data_in while the job is
--pending.
--
--This function is meant to be used to poll if data_in has completed.
--If the first return value is slurm.SUCCESS and the second return value is
--"BUSY" (or slurm.SLURM_BB_BUSY), then the job will continue to pend and
--this function will continue to be called periodically.
--If the first return value is slurm.SUCCESS and the second return value is
--empty or any other string besides "BUSY", then job and burst buffer state
--will proceed. If the first return value is not slurm.SUCCESS, then the job
--will be placed in a held state.
--
--If this function returns slurm.SUCCESS, slurm.SLURM_BB_BUSY for longer than
--StageInTimeout, then the job will be placed in a held state.
--]]
function slurm_bb_test_data_in(job_id, job_script, uid, gid, job_info)
	local work_dir = get_work_dir(job_info)

	slurm.log_info("%s: slurm_bb_test_data_in(). job id:%s, job script:%s, uid:%s, gid:%s, work_dir:%s", lua_script_name, job_id, job_script, uid, gid, work_dir)

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid, work_dir)
	if err ~= nil or conduit_jobs == nil then
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	-- get transferIDs from conduit and attach them to their respective job
	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_PRE then
			local transferID, err = j:getTransferIDFromConduit()
			if err ~= "" then
				return slurm.ERROR, "failed to get transfer ID for directive " .. j.jobIndex .. " " .. err
			end
			conduit_jobs[i].transferID = transferID
		end
	end

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_PRE then
			local cmd, args = j:stateCmd()
			slurm.log_debug("cmd: %s %s", cmd, table.concat(args, " "))
			local done, state = exec_cmd(cmd, args)

			-- check if our describe command failed (this should really never happen unless there is a network issue)
			if done == false then
				slurm.log_debug(string.format("%s: slurm_bb_test_data_in(), jobIndex=%s, output=%s : failed to get transfer state", lua_script_name, j.jobIndex, tostring(state)))

				local response = "failed to get state for directive " .. j.jobIndex
				return slurm.ERROR, response
			end


			-- make sure we get back a state that's a string
			if type(state) ~= type("") then
				slurm.log_debug(string.format("%s: slurm_bb_test_data_in(), jobIndex=%s, output=%s : failed to get transfer state", lua_script_name, j.jobIndex, tostring(state)))
				return slurm.ERROR, "failed to run transfer status command for directive " .. j.jobIndex .. " received invalid transfer state: " .. tostring(state)
			end
			state = string.gsub(state, "%s", "")
			state = string.gsub(state, "\"", "")

			if state ~= "TRANSFER_ERROR" and state ~= "TRANSFER_FINALIZED" and state ~= "TRANSFER_ABORT" and state ~= "TRANSFER_ABORTED" then
				return slurm.SUCCESS, slurm.SLURM_BB_BUSY
			elseif state == "TRANSFER_ERROR" or state == "TRANSFER_ABORT" or state == "TRANSFER_ABORTED" then
				local response = "transfer failed for directive " .. j.jobIndex
				local errDone, err = exec_cmd(j:errorCmd())
				local errMessageDone, errMessage = exec_cmd(j:errorMessageCmd())

				if errDone == true and errMessageDone == true then
					response = response .. ": " .. err .. " " .. errMessage
				end
				response = response .. ": " .. table.concat(j.userArgs, " ")
				return slurm.ERROR, response
			end
		end
	end

	return slurm.SUCCESS, ""
end

--[[
--slurm_bb_real_size
--
--This function is called asynchronously and is not required to return quickly.
--This function is called immediately after slurm_bb_test_data_in while the job
--is pending.
--
--This function is only called if pools are specified and the job requested a
--pool. This function may return a number (surrounded by quotes to make it a
--string) as the second return value. If it does, the job's usage of the pool
--will be changed to this number. A commented out example is given.
--]]
function slurm_bb_real_size(job_id, uid, gid, job_info)
	slurm.log_info("slurm_bb_real_size(). job id:%s, uid:%s, gid:%s", job_id, uid, gid)
	--return slurm.SUCCESS, "10000"
	return slurm.SUCCESS
end

--[[
--slurm_bb_paths
--
--WARNING: This function is called synchronously from slurmctld and must
--return quickly.
--This function is called after the job is scheduled but before the
--job starts running when the job is in a "running + configuring" state.
--
--The file specified by path_file is an empty file. If environment variables are
--written to path_file, these environment variables are added to the job's
--environment. A commented out example is given.
--]]
function slurm_bb_paths(job_id, job_script, path_file, uid, gid, job_info)
	slurm.log_info("slurm_bb_paths(). job id:%s, job script:%s, path file:%s, uid:%s, gid:%s", job_id, job_script, path_file, uid, gid)
	--io.output(path_file)
	--io.write("FOO=BAR")
	return slurm.SUCCESS
end

--[[
--slurm_bb_pre_run
--
--This function is called asynchronously and is not required to return quickly.
--This function is called after the job is scheduled but before the
--job starts running when the job is in a "running + configuring" state.
--]]
function slurm_bb_pre_run(job_id, job_script, uid, gid, job_info)
	slurm.log_info("slurm_bb_pre_run(). job id:%s, job script:%s, uid:%s, gid:%s", job_id, job_script, uid, gid)

	return slurm.SUCCESS, ""
end

--[[
--slurm_bb_post_run
--
--This function is called asynchronously and is not required to return quickly.
--This function is called after the job finishes. The job is in a "stage out"
--state.
--]]
function slurm_bb_post_run(job_id, job_script, uid, gid, job_info)
	slurm.log_info("slurm_post_run(). job id:%s, job script:%s, uid:%s, gid:%s", job_id, job_script, uid, gid)
	-- local rc, ret_str = sleep_wrapper(1)
	return slurm.SUCCESS, ""
end

--[[
--slurm_bb_data_out
--
--This function is called asynchronously and is not required to return quickly.
--This function is called after the job finishes immediately after
--slurm_bb_post_run. The job is in a "stage out" state.
--]]
function slurm_bb_data_out(job_id, job_script, uid, gid, job_info)
	local work_dir = get_work_dir(job_info)

	slurm.log_info("slurm_bb_data_out(). job id:%s, job script:%s, uid:%s, gid:%s, work_dir:%s", job_id, job_script, uid, gid, work_dir)

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid, work_dir)
	if err ~= nil or conduit_jobs == nil then
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_POST then
			local cmd, args = j:transferCmd()
			slurm.log_debug(table.concat(args, " "))
			local done, output = exec_cmd(cmd, args)
			-- make sure we get back a transferID that's a string
			if type(output) ~= "string" then
				slurm.log_debug(string.format("%s: slurm_bb_data_out(), jobIndex=%s, output=%s : failed to run transfer command", lua_script_name, j.jobIndex, tostring(output)))
				return slurm.ERROR, "failed to run transfer command for directive " .. j.jobIndex .. " received invalid output: " .. tostring(output)
			end

			local transferID = Extract_uuid(output)

			if done == false then
				slurm.log_debug(string.format("%s: slurm_bb_data_out(), jobIndex=%s, output=%s : failed to run transfer command", lua_script_name, j.jobIndex, tostring(output)))

				local response = "transfer failed for directive " .. j.jobIndex .. ": " .. output

				if Is_uuid(transferID) then
					j.transferID = transferID

					local eCmd, eArgs = j:errorCmd()
					local emCmd, emArgs = j:errorMessageCmd()
					local errDone, err = exec_cmd(eCmd, eArgs)
					local errMessageDone, errMessage = exec_cmd(emCmd, emArgs)

					slurm.log_debug(string.format("%s: slurm_bb_data_out(), errDone=[%s], err=[%s]", lua_script_name, tostring(errDone), tostring(err)))
					slurm.log_debug(string.format("%s: slurm_bb_data_out(), errMessageDone=[%s], errMessage=[%s]", lua_script_name, tostring(errMessageDone), tostring(errMessage)))

					if errDone == true and errMessageDone == true then
						response = response .. ": " .. err .. " " .. errMessage
					end
				end

				response = response .. ": " .. table.concat(j.userArgs, " ")
				return slurm.ERROR, response
			end

			if not Is_uuid(transferID) then
				return slurm.ERROR, "failed to run transfer command for directive " .. j.jobIndex .. " received invalid transferID: " .. tostring(output)
			end

			j.transferID = transferID
		end
	end

	return slurm.SUCCESS, ""
end

--[[
--slurm_bb_test_data_out
--
--This function is called asynchronously and is not required to return quickly.
--This function is called immediately after slurm_bb_data_out while the job is
--pending.
--
--This function is meant to be used to poll if data_out has completed.
--If the first return value is slurm.SUCCESS and the second return value is
--"BUSY" (or slurm.SLURM_BB_BUSY), then the job will stay in the completing
--state and this function will continue to be called periodically.
--If the first return value is slurm.SUCCESS and the second return value is
--empty or any other string besides "BUSY", then job and burst buffer state
--will proceed. If the first return value is not slurm.SUCCESS, then the job
--will be placed in a held state.
--]]
function slurm_bb_test_data_out(job_id, job_script, uid, gid, job_info)
	local work_dir = get_work_dir(job_info)

	slurm.log_info("%s: slurm_bb_test_data_out(). job id:%s, job script:%s, uid:%s, gid:%s, work_dir:%s", lua_script_name, job_id, job_script, uid, gid, work_dir)

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid, work_dir)
	if err ~= nil or conduit_jobs == nil then
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	-- get transferIDs from conduit and attach them to their respective job
	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_POST then
			local transferID, err = j:getTransferIDFromConduit()
			if err ~= "" then
				return slurm.ERROR, "failed to get transfer ID for directive " .. j.jobIndex .. " " .. err
			end
			conduit_jobs[i].transferID = transferID
		end
	end

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_POST then
			local cmd, args = j:stateCmd()
			slurm.log_debug("cmd: %s %s", cmd, table.concat(args, " "))
			local done, state = exec_cmd(cmd, args)

			-- check if our describe command failed (this should really never happen unless there is a network issue)
			if done == false then
				slurm.log_error(string.format("%s: slurm_bb_test_data_out(), jobIndex=%s, output=%s : failed to get transfer state", lua_script_name, j.jobIndex, tostring(state)))

				local response = "failed to get state for directive " .. j.jobIndex
				return slurm.ERROR, response
			end


			-- make sure we get back a state that's a string
			if type(state) ~= type("") then
				slurm.log_error(string.format("%s: slurm_bb_test_data_out(), jobIndex=%s, output=%s : failed to get transfer state", lua_script_name, j.jobIndex, tostring(state)))
				return slurm.ERROR, "failed to run transfer status command for directive " .. j.jobIndex .. " received invalid transfer state: " .. tostring(state)
			end
			state = string.gsub(state, "%s", "")
			state = string.gsub(state, "\"", "")

			if state ~= "TRANSFER_ERROR" and state ~= "TRANSFER_FINALIZED" and state ~= "TRANSFER_ABORT" and state ~= "TRANSFER_ABORTED" then
				return slurm.SUCCESS, slurm.SLURM_BB_BUSY
			elseif state == "TRANSFER_ERROR" or state == "TRANSFER_ABORT" or state == "TRANSFER_ABORTED" then
				local response = "transfer failed for directive " .. j.jobIndex

				local errDone, err = exec_cmd(j:errorCmd())
				local errMessageDone, errMessage = exec_cmd(j:errorMessageCmd())

				if errDone == true and errMessageDone == true then
					response = response .. ": " .. err .. " " .. errMessage
				end
				response = response .. ": " .. table.concat(j.userArgs, " ")
				return slurm.ERROR, response
			end
		end
	end

	return slurm.SUCCESS, ""
end

local function parse_json_string_array(s)
	if type(s) ~= "string" then
		return nil, "input must be a string"
	end

	local values = {}
	local i = 1
	local n = #s

	local function skip_ws()
		while i <= n and s:sub(i, i):match("%s") do
			i = i + 1
		end
	end

	skip_ws()

	if s:sub(i, i) ~= "[" then
		return nil, "expected JSON array"
	end
	i = i + 1

	skip_ws()

	if s:sub(i, i) == "]" then
		return values, ""
	end

	while i <= n do
		skip_ws()

		if s:sub(i, i) ~= '"' then
			return nil, "expected JSON string at byte " .. tostring(i)
		end
		i = i + 1

		local out = {}
		local closed = false

		while i <= n do
			local c = s:sub(i, i)
			i = i + 1

			if c == '"' then
				closed = true
				break
			elseif c == "\\" then
				if i > n then
					return nil, "unterminated JSON escape"
				end

				local e = s:sub(i, i)
				i = i + 1

				if e == '"' or e == "\\" or e == "/" then
					out[#out + 1] = e
				elseif e == "n" then
					out[#out + 1] = "\n"
				elseif e == "r" then
					out[#out + 1] = "\r"
				elseif e == "t" then
					out[#out + 1] = "\t"
				elseif e == "u" then
					local hex = s:sub(i, i + 3)
					if not hex:match("^%x%x%x%x$") then
						return nil, "invalid unicode escape"
					end
					-- Preserve unicode escapes as-is. Not needed for transferID/state/error.
					out[#out + 1] = "\\u" .. hex
					i = i + 4
				else
					return nil, "invalid JSON escape: \\" .. e
				end
			else
				out[#out + 1] = c
			end
		end

		if not closed then
			return nil, "unterminated JSON string"
		end

		values[#values + 1] = table.concat(out)

		skip_ws()

		local sep = s:sub(i, i)
		if sep == "," then
			i = i + 1
		elseif sep == "]" then
			i = i + 1
			return values, ""
		else
			return nil, "expected ',' or ']' at byte " .. tostring(i)
		end
	end

	return nil, "unterminated JSON array"
end

local function format_bbstat_rows(status)
	local values, err = parse_json_string_array(status)
	if values == nil then
		return nil, err
	end

	if #values == 0 then
		return "No matching Conduit transfers found"
	end

	local fields_per_row = 3
	if (#values % fields_per_row) ~= 0 then
		return nil, "expected transfer status fields in groups of 3, received " .. tostring(#values)
	end

	local lines = {}
	lines[#lines + 1] = string.format("%-36s  %-24s  %s", "TRANSFER_ID", "STATE", "ERROR")

	for i = 1, #values, fields_per_row do
		local transfer_id = values[i] or ""
		local state = values[i + 1] or ""
		local transfer_error = values[i + 2] or ""

		lines[#lines + 1] = string.format(
			"%-36s  %-24s  %s",
			transfer_id,
			state,
			transfer_error
		)
	end

	return table.concat(lines, "\n")
end

local function ensure_trailing_newline(s)
	if type(s) ~= "string" then
		s = tostring(s)
	end

	if s == "" or s:sub(-1) ~= "\n" then
		return s .. "\n"
	end

	return s
end


--[[
--slurm_bb_get_status
--
--This function is called asynchronously and is not required to return quickly.
--
--This function is called when "scontrol show bbstat" is run. It receives the
--authenticated user id and group id of the caller, as well as a variable
--number of arguments - whatever arguments are after "bbstat".
--For example:
--
--  scontrol show bbstat foo bar
--
--This command will pass 2 arguments after uid and gid to this function:
--  "foo" and "bar".
--
--If this function returns slurm.SUCCESS, then this function's second return
--value will be printed where the scontrol command was run. If this function
--returns slurm.ERROR, then this function's second return value is ignored and
--an error message will be printed instead.
--]]
function slurm_bb_get_status(uid, gid, ...)
	local args = { ... }
	local argc = select("#", ...)

	local function finish(msg)
		return slurm.SUCCESS, ensure_trailing_newline(msg)
	end

	if argc ~= 2 or args[1] ~= "conduit" then
		local msg = "Usage: conduit <slurm-job-id>"
		slurm.log_debug("%s: slurm_bb_get_status(%s): %s", lua_script_name, table.concat(args, ", "), msg)
		return finish(msg)
	end

	local jid = args[2]
	if string.find(jid, "^%d+$") == nil then
		local msg = "A job ID must contain only digits."
		slurm.log_debug("%s: slurm_bb_get_status(%s): %s", lua_script_name, table.concat(args, ", "), msg)
		return finish(msg)
	end

	local cmd, cmd_args = SlurmIDStateCmd(jid, uid)
	slurm.log_debug("cmd: %s %s", cmd, table.concat(cmd_args, " "))

	local done, status = exec_cmd(cmd, cmd_args)

	if type(status) ~= "string" then
		local msg = "failed to run status command: received invalid output: " .. tostring(status)
		slurm.log_error("%s: slurm_bb_get_status(%s): %s", lua_script_name, table.concat(args, ", "), msg)
		return finish(msg)
	end

	local compact = status:gsub("%s", "")
	if done == true then
		if compact == "" or compact == "[]" then
			return finish("No Conduit transfers found for Slurm job " .. jid)
		end

		local formatted, format_err = format_bbstat_rows(status)
		if formatted ~= nil then
			return finish(formatted)
		end

		slurm.log_error("%s: slurm_bb_get_status(%s): failed to format status output: %s",
			lua_script_name, table.concat(args, ", "), tostring(format_err))

		return finish(status)
	end

	local msg = string.format(
		"failed to run status command: %s %s: %s",
		cmd,
		table.concat(cmd_args, " "),
		status
	)

	slurm.log_error("%s: slurm_bb_get_status(%s): %s", lua_script_name, table.concat(args, ", "), msg)

	-- Status command only: return SUCCESS so scontrol prints the useful message.
	return finish(msg)
end
