-- Copyright 2026. Triad National Security, LLC. All rights reserved.

-- ====== CONFIG ======
local CONDUIT_CLI = "/usr/sbin/conduit"
local CONDUIT_CERT = "/etc/conduit/conduit-slurm-cert.pem"
local CONDUIT_KEY = "/etc/conduit/conduit-slurm-key.pem"
local CONDUIT_CA = "/etc/conduit/conduit-external-ca.pem"
local CONDUIT_CLI_CONFIG = "/etc/conduit/conduit-cli-config.yaml"


local STATIC_FLAGS = { "--cert", CONDUIT_CERT, "--key", CONDUIT_KEY, "--ca", CONDUIT_CA, "--config", CONDUIT_CMD_CONFIG }


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
}
CONDUIT_JOB.__index = CONDUIT_JOB

function CONDUIT_JOB:new(jobID, jobType, jobIndex, userArgs, uid)
	local job = {}
	setmetatable(job, self)
	self.__index = self
	job.jobID = jobID    -- slurm jobid
	job.jobType = jobType -- CONDUIT_PRE or CONDUIT_POST
	job.jobIndex = jobIndex -- the index of the job in the sbatch file
	job.uid = uid        -- user id
	job.userArgs = userArgs -- provided transfer command
	job.transferID = ""
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
	table.insert(final_args, self.uid)

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CMD, final_args
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
	table.insert(final_args, self.uid)

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CMD, final_args
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
	table.insert(final_args, self.uid)

	table.insert(final_args, "--comment")
	table.insert(final_args, self:comment())

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CMD, final_args
end

-- requires a transferID in CONDUIT_JOB
function CONDUIT_JOB:abortCmd()
	local final_args = {}

	table.insert(final_args, "abort")
	table.insert(final_args, self.transferID)

	table.insert(final_args, "--user")
	table.insert(final_args, self.uid)

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CMD, final_args
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
	table.insert(final_args, self.uid)

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CMD, final_args
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
		slurm.log_error(string.format("%s: getTransferIDFromConduit(), jobIndex=%s, transferID=%s : failed to get transfer ID from conduit", lua_script_name, self.jobIndex, output))
		return "", "failed to get transferID from conduit with comment: " .. self:comment() .. " received invalid transferID: " .. output
	end

	local transferID = string.gsub(output, "%s", "")
	transferID = string.gsub(transferID, "\"", "")

	if not Is_uuid(transferID) then
		slurm.log_error(string.format("%s: getTransferIDFromConduit(), jobIndex=%s, transferID=%s : failed to get transfer ID from conduit", lua_script_name, self.jobIndex, output))
		return "", "failed to get transferID from conduit with comment: " .. self:comment() .. " received invalid transferID: " .. output
	end

	return transferID, ""
end

function SlurmIDStateCmd(slurmJobID, uid)
	local jsonpath = "$..[transferID,state]"

	local final_args = {}

	table.insert(final_args, "describe")
	table.insert(final_args, "--quiet")

	table.insert(final_args, slurmJobID)

	table.insert(final_args, "--jsonpath")
	table.insert(final_args, jsonpath)

	table.insert(final_args, "--user")
	table.insert(final_args, uid)

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	return CONDUIT_CMD, final_args
end

function TransferExistsCmd(comment, uid)
	local final_args = {}

	table.insert(final_args, "describe")
	table.insert(final_args, "--quiet")

	table.insert(final_args, comment)

	table.insert(final_args, "--user")
	table.insert(final_args, uid)

	-- add static flags to the end of final_args
	for i = 1, #STATIC_FLAGS do
		final_args[#final_args + 1] = STATIC_FLAGS[i]
	end

	local done, output = exec_cmd(CONDUIT_CMD, final_args)
	if done == false then
		return false, "failed to get transfer from conduit with comment: " .. comment .. " :command failed: " .. output
	end
	-- make sure we get back an output that's a string
	if type(output) ~= type("") then
		return false, "failed to get transferID from conduit with comment: " .. comment .. " received invalid output: " .. output
	end
	output = string.gsub(output, "%s", "")
	output = string.gsub(output, "\"", "")

	if output == "[]" then
		return false, ""
	end

	return true, ""
end

-- io_popen will run the given command and collect its output.
-- On success this returns true and the output of the command.
-- On failure this returns false and the output of the command.
function io_popen(cmd)
	local handle = io.popen(cmd)
	if handle == nil then
		slurm.log_debug("nil handle")
		return false, nil
	end
	local result = handle:read("*a")
	-- The exit status is an integer in rc[3].
	local rc = { handle:close() }
	if rc[3] ~= 0 then
		return false, result
	end
	return true, result
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

	local pid = posix.fork()
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
	local results = {}
	local contents
	job_id = job_info["job_id"]
	slurm.log_info("%s: slurm_bb_job_process(). job_script=%s, uid=%s, gid=%s, job_id=%s", lua_script_name, job_script, uid, gid, job_id)
	io.input(job_script)
	contents = io.read("*all")

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid)
	if err ~= nil or conduit_jobs == nil then
		slurm.log_error("failed to parse directives: " .. err)
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_PRE then
			local cmd, args = j:validationCmd()
			slurm.log_debug(table.concat(args, " "))
			local done, output = exec_cmd(cmd, args)
			slurm.log_debug("validation command complete")
			-- remove newlines
			output = output:gsub("[\r\n]+", "")
			-- make sure we get back a transferID that's a string
			if type(output) ~= type("") then
				slurm.log_debug(string.format("%s: slurm_bb_process(), jobIndex=%s, transferID=%s : failed to run validation command", lua_script_name, j.jobIndex, output))
				return slurm.ERROR, "failed to run validation command for directive " .. j.jobIndex .. " received invalid transferID: " .. output
			end
			j.transferID = string.gsub(output, "%s", "")
			j.transferID = string.gsub(j.transferID, "\"", "")

			if not Is_uuid(j.transferID) then
				slurm.log_debug(string.format("%s: slurm_bb_process(), jobIndex=%s, transferID=%s : failed to run validation command", lua_script_name, j.jobIndex, output))
				return slurm.ERROR, "failed to run validation command for directive " .. j.jobIndex .. " received invalid transferID: " .. output
			end

			slurm.log_debug("successfully sent validation to conduit: " .. output)

			-- done == false means the command failed
			if done == false then
				slurm.log_debug(string.format("%s: slurm_bb_process(), jobIndex=%s: %s", lua_script_name, j.jobIndex, output))

				local response = "validation failed for directive " .. j.jobIndex .. ": " .. output

				local eCmd, eArgs = j:errorCmd()
				local emCmd, emArgs = j:errorMessageCmd()
				slurm.log_debug(table.concat(eArgs, " "))
				slurm.log_debug(table.concat(emArgs, " "))

				local errDone, err = exec_cmd(eCmd, eArgs)
				local errMessageDone, errMessage = exec_cmd(emCmd, emArgs)

				slurm.log_debug(err)
				slurm.log_debug(errMessage)


				if errDone == true and errMessageDone == true then
					response = response .. ": " .. err .. " " .. errMessage
				end
				return slurm.ERROR, response
			end
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

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid)
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
			transferID, err = j:getTransferIDFromConduit()
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
				slurm.log_error(string.format("%s: slurm_bb_job_teardown(), jobIndex=%s, transferID=%s : failed to get transfer active state", lua_script_name, j.jobIndex, transferID))
				return slurm.ERROR, "failed to run transfer active status command for directive " .. j.jobIndex .. " received invalid active state: " .. transferID
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
					-- local done, transferID = io_popen("echo 3cb72039-e271-4ba4-bb03-74b28d7ad80" .. i)
					if done == false then
						slurm.log_error(string.format("%s: slurm_bb_job_teardown(), jobIndex=%s, transferID=%s : failed to get transfer active state", lua_script_name, j.jobIndex, transferID))

						local response = "failed to abort transfer for directive " .. j.jobIndex .. " transferid: " .. j.transferID
						return slurm.ERROR, response
					end
				else
					-- we're not in a hurry, wait for the transfer to complete
					local cmd, args = j:watchCmd()
					slurm.log_debug(table.concat(args, " "))
					local done, transferID = exec_cmd(cmd, args)
					-- local done, transferID = io_popen("echo 3cb72039-e271-4ba4-bb03-74b28d7ad80" .. i)
					if done == false then
						slurm.log_error(string.format("%s: slurm_bb_job_teardown(), jobIndex=%s, transferID=%s : failed to get transfer active state", lua_script_name, j.jobIndex, transferID))

						local response = "failed to watch state for directive " .. j.jobIndex .. " transferid: " .. j.transferID
						return slurm.ERROR, response
					end
				end
			end
		end
	end

	slurm.log_info("all transfers teardown successful, sending scancel")
	local done, output = Scancel(job_id, hurry)
	if done then
		slurm.log_debug(string.format("successfully sent scancel command for job %s %s", job_id, transferID))
	else
		local response = string.format("failed to send scancel command for job %s %s: %s", job_id, transferID, output)
		slurm.log_error(response)
		return slurm.ERROR, response
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
	slurm.log_info("slurm_bb_data_in(). job id:%s, job script:%s, uid:%s, gid:%s", job_id, job_script, uid, gid)


	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid)
	if err ~= nil or conduit_jobs == nil then
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	slurm.log_debug("parsed conduit directives successfully")

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_PRE then
			local cmd, args = j:transferCmd()
			slurm.log_debug(table.concat(args, " "))
			local done, transferID = exec_cmd(cmd, args)
			-- local done, transferID = io_popen("echo 3cb72039-e271-4ba4-bb03-74b28d7ad80" .. i)
			-- make sure we get back a transferID that's a string
			if type(transferID) ~= type("") then
				slurm.log_debug(string.format("%s: slurm_bb_data_in(), jobIndex=%s, transferID=%s : failed to run transfer command", lua_script_name, j.jobIndex, transferID))
				return slurm.ERROR, "failed to run transfer command for directive " .. j.jobIndex .. " received invalid transferID: " .. transferID
			end
			j.transferID = string.gsub(transferID, "%s", "")
			j.transferID = string.gsub(j.transferID, "\"", "")

			if not Is_uuid(j.transferID) then
				slurm.log_debug(string.format("%s: slurm_bb_data_in(), jobIndex=%s, transferID=%s : failed to run transfer command", lua_script_name, j.jobIndex, transferID))
				return slurm.ERROR, "failed to run transfer command for directive " .. j.jobIndex .. " received invalid transferID: " .. transferID
			end

			-- done = false
			if done == false then
				slurm.log_debug(string.format("%s: slurm_bb_data_in(), jobIndex=%s, transferID=%s : failed to run transfer command", lua_script_name, j.jobIndex, transferID))

				local response = "transfer failed for directive " .. j.jobIndex

				local eCmd, eArgs = j:errorCmd()
				local emCmd, emArgs = j:errorMessageCmd()
				slurm.log_debug(table.concat(eArgs, " "))
				slurm.log_debug(table.concat(emArgs, " "))
				local errDone, err = exec_cmd(eCmd, eArgs)
				local errMessageDone, errMessage = exec_cmd(emCmd, emArgs)

				slurm.log_debug(string.format("%s: slurm_bb_data_in(), errDone=[%s], err=[%s]", lua_script_name, errDone, err))
				slurm.log_debug(string.format("%s: slurm_bb_data_in(), errMessageDone=[%s], errMessage=[%s]", lua_script_name, errMessageDone, errMessage))

				-- errDone = true
				-- errMessageDone = true
				-- err = "CONDUIT_ERROR"
				-- errMessage = "this is a conduit error message"

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
	slurm.log_info("%s: slurm_bb_test_data_in(). job id:%s, job script:%s, uid:%s, gid:%s", lua_script_name, job_id, job_script, uid, gid)

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid)
	if err ~= nil or conduit_jobs == nil then
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	-- get transferIDs from conduit and attach them to their respective job
	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_PRE then
			transferID, err = j:getTransferIDFromConduit()
			if err ~= "" then
				return slurm.ERROR, "failed to get transfer ID for directive " .. j.jobIndex .. " " .. err
			end
			conduit_jobs[i].transferID = transferID
		end
	end

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_PRE then
			slurm.log_debug(j:stateCmd())
			local done, state = exec_cmd(j:stateCmd())
			-- local done, state = io_popen("echo TRANSFER_ERROR" .. i)

			-- check if our describe command failed (this should really never happen unless there is a network issue)
			if done == false then
				slurm.log_debug(string.format("%s: slurm_bb_test_data_in(), jobIndex=%s, transferID=%s : failed to get transfer state", lua_script_name, j.jobIndex, transferID))

				local response = "failed to get state for directive " .. j.jobIndex
				return slurm.ERROR, response
			end


			-- make sure we get back a state that's a string
			if type(state) ~= type("") then
				slurm.log_debug(string.format("%s: slurm_bb_test_data_in(), jobIndex=%s, transferID=%s : failed to get transfer state", lua_script_name, j.jobIndex, transferID))
				return slurm.ERROR, "failed to run transfer status command for directive " .. j.jobIndex .. " received invalid transfer state: " .. transferID
			end
			state = string.gsub(state, "%s", "")
			state = string.gsub(state, "\"", "")

			slurm.log_debug(state)

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
	slurm.log_info("slurm_bb_real_size(). job id:%s, uid:%s, gid:%s",
		job_id, uid, gid)
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
	slurm.log_info("slurm_bb_paths(). job id:%s, job script:%s, path file:%s, uid:%s, gid:%s",
		job_id, job_script, path_file, uid, gid)
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
	slurm.log_info("slurm_bb_pre_run(). job id:%s, job script:%s, uid:%s, gid:%s",
		job_id, job_script, uid, gid)

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
	slurm.log_info("slurm_post_run(). job id:%s, job script:%s, uid:%s, gid:%s",
		job_id, job_script, uid, gid)
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
	slurm.log_info("slurm_bb_data_out(). job id:%s, job script:%s, uid:%s, gid:%s", job_id, job_script, uid, gid)


	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid)
	if err ~= nil or conduit_jobs == nil then
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_POST then
			local cmd, args = j:transferCmd()
			slurm.log_debug(table.concat(args, " "))
			local done, transferID = exec_cmd(cmd, args)
			-- local done, transferID = io_popen("echo 3cb72039-e271-4ba4-bb03-74b28d7ad80" .. i)
			-- make sure we get back a transferID that's a string
			if type(transferID) ~= type("") then
				slurm.log_debug(string.format("%s: slurm_bb_data_out(), jobIndex=%s, transferID=%s : failed to run transfer command", lua_script_name, j.jobIndex, transferID))
				return slurm.ERROR, "failed to run transfer command for directive " .. j.jobIndex .. " received invalid transferID: " .. transferID
			end
			j.transferID = string.gsub(transferID, "%s", "")
			j.transferID = string.gsub(j.transferID, "\"", "")

			if not Is_uuid(j.transferID) then
				slurm.log_debug(string.format("%s: slurm_bb_data_out(), jobIndex=%s, transferID=%s : failed to run transfer command", lua_script_name, j.jobIndex, j.transferID))
				return slurm.ERROR, "failed to run transfer command for directive " .. j.jobIndex .. " received invalid transferID: " .. transferID
			end

			-- done = false
			if done == false then
				slurm.log_debug(string.format("%s: slurm_bb_data_out(), jobIndex=%s, transferID=%s : failed to run transfer command", lua_script_name, j.jobIndex, transferID))

				local response = "transfer failed for directive " .. j.jobIndex
				local errDone, err = exec_cmd(j:errorCmd())
				local errMessageDone, errMessage = exec_cmd(j:errorMessageCmd())

				-- errDone = true
				-- errMessageDone = true
				-- err = "CONDUIT_ERROR"
				-- errMessage = "this is a conduit error message"

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
	slurm.log_info("%s: slurm_bb_test_data_out(). job id:%s, job script:%s, uid:%s, gid:%s", lua_script_name, job_id, job_script, uid, gid)

	local conduit_jobs, err = parse_conduit_directives(job_script, job_id, uid)
	if err ~= nil or conduit_jobs == nil then
		return slurm.ERROR, "failed to parse directives: " .. err
	end

	-- get transferIDs from conduit and attach them to their respective job
	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_POST then
			transferID, err = j:getTransferIDFromConduit()
			if err ~= "" then
				return slurm.ERROR, "failed to get transfer ID for directive " .. j.jobIndex .. " " .. err
			end
			conduit_jobs[i].transferID = transferID
		end
	end

	for i, j in pairs(conduit_jobs) do
		if j.jobType == CONDUIT_POST then
			slurm.log_debug(j:stateCmd())
			local done, state = exec_cmd(j:stateCmd())
			-- local done, state = io_popen("echo TRANSFER_ERROR" .. i)

			-- check if our describe command failed (this should really never happen unless there is a network issue)
			if done == false then
				slurm.log_error(string.format("%s: slurm_bb_test_data_out(), jobIndex=%s, transferID=%s : failed to get transfer state", lua_script_name, j.jobIndex, transferID))

				local response = "failed to get state for directive " .. j.jobIndex
				return slurm.ERROR, response
			end


			-- make sure we get back a state that's a string
			if type(state) ~= type("") then
				slurm.log_error(string.format("%s: slurm_bb_test_data_out(), jobIndex=%s, transferID=%s : failed to get transfer state", lua_script_name, j.jobIndex, transferID))
				return slurm.ERROR, "failed to run transfer status command for directive " .. j.jobIndex .. " received invalid transfer state: " .. transferID
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
	--slurm.log_info("%s: slurm_bb_get_status(). uid:%s, gid:%s", lua_script_name, uid, gid)

	local ret = slurm.ERROR
	local msg = "Usage: conduit <slurm-job-id>"

	-- Create a table from variable arg list
	-- NOTE: The args[] array index begins at 1.
	local args = { ... }
	args.n = select("#", ...)

	local found_jid = false
	local jid = 0
	if args.n == 2 and args[1] == "conduit" then
		jid = args[2]
		found_jid = true
	end
	if found_jid == true then
		local done = false
		local status = ""
		if string.find(jid, "^%d+$") == nil then
			msg = "A job ID must contain only digits."
		else
			done, status = exec_cmd(SlurmIDStateCmd(jid, uid))
		end
		if done == true then
			ret = slurm.SUCCESS
			msg = status
		else
			msg = "failed to run status command: " .. SlurmIDStateCmd(jid, uid)
		end
	end

	if ret == slurm.ERROR then
		slurm.log_error("%s: slurm_bb_get_status(%s): %s", lua_script_name, table.concat(args, ", "), msg)
	end
	return ret, msg
end

local function tokenize(s)
	if type(s) ~= "string" then return nil, "input must be a string" end
	-- if has_control(s) then return nil, "control characters not allowed" end
	local t = {}
	for tok in s:gmatch("%S+") do t[#t + 1] = tok end
	if #t == 0 then return nil, "empty directive" end
	return t
end


function parse_conduit_directives(job_script, jobID, uid)
	Conduit_directives = {}
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
			local toks, err = tokenize(cmd); if not toks then return false, err end

			Job = CONDUIT_JOB:new(jobID, jobType, idx, toks, uid)

			-- create conduit job
			Conduit_directives[idx] = Job
			idx = idx + 1
		end
	end

	return Conduit_directives
end

function Is_uuid(str)
	-- optional single or double quote at start and end
	return str:match([[^["']?%x%x%x%x%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x["']?$]]) ~= nil
end

-- Scancel will run the Slurm scancel command and collect its output.
-- On success this returns true and the output of the command.
-- On failure this returns false and the output of the command.
function Scancel(jobId, hurry)
	local hurry_opt = ""
	if hurry == true then
		hurry_opt = "--hurry "
	end
	local scmd = "scancel " .. hurry_opt .. jobId
	return io_popen(scmd)
end
