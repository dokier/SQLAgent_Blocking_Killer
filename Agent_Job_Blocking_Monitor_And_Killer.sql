USE [NSP_TEMP]
GO

/****** Object:  StoredProcedure [dbo].[Agent_Job_Blocking_Monitor_And_Killer]    Script Date: 5/27/2025 7:58:17 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER PROCEDURE [dbo].[Agent_Job_Blocking_Monitor_And_Killer]
    @SecondsThreshold INT = 30,
    @SampleIntervalSeconds INT = 5,
    @MinSnapshotPercent INT = 90  -- % of samples a blocker must appear in to be considered confirmed
AS
BEGIN TRY
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE @TotalSnapshotsExpected INT = CEILING(1.0 * @SecondsThreshold / @SampleIntervalSeconds);

    IF NOT EXISTS (
        SELECT 1
        FROM msdb.dbo.sysjobactivity ja
        JOIN msdb.dbo.sysjobs j ON ja.job_id = j.job_id
        JOIN NSP_TEMP.dbo.Agent_Job_Blocking_Whitelist wl ON j.name = wl.JobName
        WHERE ja.start_execution_date IS NOT NULL
          AND ja.stop_execution_date IS NULL
          AND wl.IsActive = 1
    )
        RETURN;

    IF OBJECT_ID('tempdb..#BlockerSnapshots') IS NOT NULL DROP TABLE #BlockerSnapshots;

    CREATE TABLE #BlockerSnapshots (
        SnapshotTime DATETIME2(3) NOT NULL,
        BlockerSessionId INT NOT NULL,
        BlockedSessionId INT NOT NULL
    );

    DECLARE @ElapsedSeconds INT = 0;

    WHILE @ElapsedSeconds < @SecondsThreshold
    BEGIN
        INSERT INTO #BlockerSnapshots (SnapshotTime, BlockerSessionId, BlockedSessionId)
        SELECT 
            SYSDATETIME(),
            r.blocking_session_id,
            r.session_id
        FROM sys.dm_exec_requests r
        JOIN sys.dm_exec_sessions s ON r.blocking_session_id = s.session_id
        WHERE r.blocking_session_id IS NOT NULL
          AND r.blocking_session_id <> 0
          AND EXISTS (
              SELECT 1
              FROM msdb.dbo.sysjobs j
              JOIN NSP_TEMP.dbo.Agent_Job_Blocking_Whitelist wl ON j.name = wl.JobName
              WHERE wl.IsActive = 1
                AND s.program_name LIKE '%SQLAgent - TSQL JobStep (Job ' + 
                    CONVERT(VARCHAR(MAX), CONVERT(BINARY(16), j.job_id), 1) + '%'
          );

        DECLARE @DelayString VARCHAR(8) = 
            RIGHT('00' + CAST(@SampleIntervalSeconds / 3600 AS VARCHAR), 2) + ':' +
            RIGHT('00' + CAST((@SampleIntervalSeconds % 3600) / 60 AS VARCHAR), 2) + ':' +
            RIGHT('00' + CAST(@SampleIntervalSeconds % 60 AS VARCHAR), 2);

        WAITFOR DELAY @DelayString;
        SET @ElapsedSeconds += @SampleIntervalSeconds;
    END

    ;WITH ConsistentBlockers AS (
        SELECT BlockerSessionId, COUNT(DISTINCT SnapshotTime) AS SeenSnapshots
        FROM #BlockerSnapshots
        GROUP BY BlockerSessionId
    ),
    ConfirmedBlockers AS (
        SELECT BlockerSessionId
        FROM ConsistentBlockers
        WHERE SeenSnapshots * 100.0 / @TotalSnapshotsExpected >= @MinSnapshotPercent
    ),
    JobSessions AS (
        SELECT 
            s.session_id,
            s.program_name,
            s.login_name,
            COALESCE(r.status,s.status) AS SessionStatus,
            DB_NAME(s.database_id) AS DatabaseName,
            r.sql_handle,
            j.name AS JobName,
            js.step_name AS JobStepName
        FROM sys.dm_exec_sessions s
        LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
        CROSS APPLY (
            SELECT TOP 1 
                j.job_id,
                j.name,
                CONVERT(VARCHAR(MAX), CONVERT(BINARY(16), j.job_id), 1) AS JobHex
            FROM msdb.dbo.sysjobs j
            JOIN NSP_TEMP.dbo.Agent_Job_Blocking_Whitelist wl ON j.name = wl.JobName
            WHERE wl.IsActive = 1
              AND s.program_name LIKE '%SQLAgent - TSQL JobStep (Job ' + 
                  CONVERT(VARCHAR(MAX), CONVERT(BINARY(16), j.job_id), 1) + '%'
        ) j
        OUTER APPLY (
            SELECT TOP 1 js.step_name
            FROM msdb.dbo.sysjobsteps js
            WHERE js.job_id = j.job_id
              AND js.step_id = TRY_CAST(
                    LTRIM(SUBSTRING(
                        s.program_name,
                        CHARINDEX(': Step', s.program_name) + 6,
                        CHARINDEX(')', s.program_name) - CHARINDEX(': Step', s.program_name) - 6
                    )) AS INT)
        ) js
        WHERE r.command NOT LIKE 'KILLED/ROLLBACK%'
    )
    SELECT DISTINCT
        s.session_id AS SPID,
        s.program_name AS ProgramName,
        s.JobName,
        s.JobStepName,
        s.DatabaseName,
        s.SessionStatus,
        s.login_name AS LoginName,
        txt.text AS CommandText
    INTO #ConfirmedBlockersToKill
    FROM ConfirmedBlockers cb
    JOIN JobSessions s ON s.session_id = cb.BlockerSessionId
    OUTER APPLY sys.dm_exec_sql_text(s.sql_handle) AS txt;

    WHILE EXISTS (SELECT 1 FROM #ConfirmedBlockersToKill)
    BEGIN
        DECLARE 
            @SPID INT,
            @ProgramName NVARCHAR(255),
            @JobName SYSNAME,
            @JobStepName NVARCHAR(255),
            @DatabaseName NVARCHAR(128),
            @SessionStatus NVARCHAR(60),
            @LoginName NVARCHAR(128),
            @CommandText NVARCHAR(MAX),
            @BlockedSessions NVARCHAR(MAX),
            @KilledBy SYSNAME = ORIGINAL_LOGIN();

        SELECT TOP (1)
            @SPID = SPID,
            @ProgramName = ProgramName,
            @JobName = JobName,
            @JobStepName = JobStepName,
            @DatabaseName = DatabaseName,
            @SessionStatus = SessionStatus,
            @LoginName = LoginName,
            @CommandText = CommandText
        FROM #ConfirmedBlockersToKill;

        -- Revalidate SPID is still blocking and still tied to an active whitelisted SQL Agent job
        IF NOT EXISTS (
            SELECT 1
            FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s ON r.blocking_session_id = s.session_id
            JOIN msdb.dbo.sysjobs j ON s.program_name LIKE '%SQLAgent - TSQL JobStep (Job ' + 
                CONVERT(VARCHAR(MAX), CONVERT(BINARY(16), j.job_id), 1) + '%'
            JOIN NSP_TEMP.dbo.Agent_Job_Blocking_Whitelist wl ON j.name = wl.JobName
            WHERE r.blocking_session_id = @SPID
              AND wl.IsActive = 1
              AND r.command NOT LIKE 'KILLED/ROLLBACK%'
        )
        BEGIN
            DELETE FROM #ConfirmedBlockersToKill WHERE SPID = @SPID;
            CONTINUE;
        END

        SELECT @BlockedSessions = STRING_AGG(CONVERT(NVARCHAR(10), BlockedSessionId), ',')
        FROM (
            SELECT DISTINCT BlockedSessionId
            FROM #BlockerSnapshots
            WHERE BlockerSessionId = @SPID
        ) AS DistinctBlocked;

        BEGIN TRY
            DECLARE @KillCommand NVARCHAR(100) = 'KILL ' + CAST(@SPID AS NVARCHAR(10));
            EXEC (@KillCommand);

            INSERT INTO NSP_TEMP.dbo.Agent_Job_Blocking_Kill_Log (
                SPID, ProgramName, JobName, JobStepName, KilledBy,
                DatabaseName, SessionStatus, LoginName, CommandText, BlockedSessions
            )
            VALUES (
                @SPID, @ProgramName, @JobName, @JobStepName, @KilledBy,
                @DatabaseName, @SessionStatus, @LoginName, @CommandText, @BlockedSessions
            );
        END TRY
        BEGIN CATCH
            EXEC NSP_TEMP.dbo.Log_Error_Message;
        END CATCH;

        DELETE FROM #ConfirmedBlockersToKill WHERE SPID = @SPID;
    END

    DROP TABLE IF EXISTS #BlockerSnapshots;
    DROP TABLE IF EXISTS #ConfirmedBlockersToKill;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    EXEC NSP_TEMP.dbo.Log_Error_Message;
    THROW;
END CATCH;
GO
