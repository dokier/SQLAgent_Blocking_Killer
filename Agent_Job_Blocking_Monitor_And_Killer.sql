USE [NSP_TEMP]
GO

/****** Object:  StoredProcedure [dbo].[Agent_Job_Blocking_Monitor_And_Killer4]    Script Date: 5/30/2025 12:55:18 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER PROCEDURE [dbo].[Agent_Job_Blocking_Monitor_And_Killer4]
    @SecondsThreshold INT = 30,
    @SampleIntervalSeconds INT = 5,
    @MinSnapshotPercent INT = 90
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
        JOIN NSP_TEMP.dbo.Agent_Job_Killable_Whitelist wl ON j.name = wl.JobName
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
              JOIN NSP_TEMP.dbo.Agent_Job_Killable_Whitelist wl ON j.name = wl.JobName
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
            JOIN NSP_TEMP.dbo.Agent_Job_Killable_Whitelist wl ON j.name = wl.JobName
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

    DECLARE @KillReport TABLE (
        SPID INT,
        JobName SYSNAME,
        JobStepName NVARCHAR(255),
        DatabaseName NVARCHAR(128),
        SessionStatus NVARCHAR(60),
        LoginName NVARCHAR(128),
        CommandText NVARCHAR(MAX),
        BlockedSessions NVARCHAR(MAX)
    );

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
            @BlockedSessionInfo NVARCHAR(MAX),
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

		 IF NOT EXISTS (
            SELECT 1
            FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s ON r.blocking_session_id = s.session_id
            JOIN msdb.dbo.sysjobs j ON s.program_name LIKE '%SQLAgent - TSQL JobStep (Job ' + 
                CONVERT(VARCHAR(MAX), CONVERT(BINARY(16), j.job_id), 1) + '%'
            JOIN NSP_TEMP.dbo.Agent_Job_Killable_Whitelist wl ON j.name = wl.JobName
            WHERE r.blocking_session_id = @SPID
              AND wl.IsActive = 1
              AND r.command NOT LIKE 'KILLED/ROLLBACK%'
        )
        BEGIN
            DELETE FROM #ConfirmedBlockersToKill WHERE SPID = @SPID;
            CONTINUE;
        END

        DECLARE @AllBlockedAreSentry BIT = 1;
        DECLARE @AnyBlockedIsAlsoBlocker BIT = 0;

        SELECT 
            @AllBlockedAreSentry = 
                CASE 
                    WHEN EXISTS (
                        SELECT 1
                        FROM sys.dm_exec_requests r
                        JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                        WHERE r.blocking_session_id = @SPID
                          AND s.program_name NOT LIKE '%Sentry%'
                    ) THEN 0 ELSE 1 
                END;

        SELECT @AnyBlockedIsAlsoBlocker = 
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM sys.dm_exec_requests r
                    WHERE r.blocking_session_id IN (
                        SELECT r2.session_id
                        FROM sys.dm_exec_requests r2
                        WHERE r2.blocking_session_id = @SPID
                    )
                )
                THEN 1 ELSE 0
            END;

        IF @AllBlockedAreSentry = 1 AND @AnyBlockedIsAlsoBlocker = 0
        BEGIN
            DELETE FROM #ConfirmedBlockersToKill WHERE SPID = @SPID;
            CONTINUE;
        END

        SELECT @BlockedSessions = STRING_AGG(CONVERT(NVARCHAR(10), r.session_id), ',')
        FROM sys.dm_exec_requests r
        WHERE r.blocking_session_id = @SPID;

        SELECT @BlockedSessionInfo = (
            SELECT 
                s.session_id AS spid,
                s.program_name,
                s.login_name,
                s.host_name,
                s.client_interface_name,
                s.status,
                DB_NAME(s.database_id) AS database_name,
                r_sql.text AS sql_text,
                r.last_wait_type,
                r.wait_resource
            FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
            OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS r_sql
            WHERE r.blocking_session_id = @SPID
            FOR JSON PATH, ROOT('BlockedSessions')
        );

        BEGIN TRY
            DECLARE @KillCommand NVARCHAR(100) = 'KILL ' + CAST(@SPID AS NVARCHAR(10));
            EXEC (@KillCommand);

            INSERT INTO NSP_TEMP.dbo.Agent_Job_Blocking_Kill_Log (
                SPID, ProgramName, JobName, JobStepName, KilledBy,
                DatabaseName, SessionStatus, LoginName, CommandText,
                BlockedSessions, SecondsThreshold, SampleIntervalSeconds, MinSnapshotPercent,
                BlockedSessionInfo
            )
            VALUES (
                @SPID, @ProgramName, @JobName, @JobStepName, @KilledBy,
                @DatabaseName, @SessionStatus, @LoginName, @CommandText,
                @BlockedSessions, @SecondsThreshold, @SampleIntervalSeconds, @MinSnapshotPercent,
                @BlockedSessionInfo
            );

            INSERT INTO @KillReport
            VALUES (
                @SPID, @JobName, @JobStepName,
                @DatabaseName, @SessionStatus, @LoginName,
                @CommandText, @BlockedSessions
            );
        END TRY
        BEGIN CATCH
            EXEC NSP_TEMP.dbo.Log_Error_Message;
        END CATCH;

        DELETE FROM #ConfirmedBlockersToKill WHERE SPID = @SPID;
    END

    IF EXISTS (SELECT 1 FROM @KillReport)
    BEGIN
        DECLARE @Body NVARCHAR(MAX) = 
        N'<style>
          td {
            padding: 2px;
            border:1px solid #4a81aa;
            font-family:Arial, Helvetica, sans-serif;
            font-size:10pt;
            word-wrap: break-word;
            overflow-wrap: break-word;
            white-space: normal;
          }
          div {
            font-family:Arial, Helvetica, sans-serif;
            font-size:10pt;
          }
        </style>
		<div style="width:100%; text-align:center; margin-bottom:10px;">
			For additional details, refer to the <strong>Agent_Job_Blocking_Kill_Log</strong> table in the <strong>My Database</strong> database.
		</div>
        <div style="width:100%;">
        <table style="border:3px solid #4a81aa; background-color:#f6f6e6; width: 1600px; margin:0px auto; border-spacing: 0px; border-collapse: collapse;" align="center">
          <tr>
            <td style="font-weight:bold;text-align:center;">SPID</td>
            <td style="font-weight:bold;text-align:center;">JobName</td>
            <td style="font-weight:bold;text-align:center;">JobStepName</td>
            <td style="font-weight:bold;text-align:center;">DatabaseName</td>
            <td style="font-weight:bold;text-align:center;">SessionStatus</td>
            <td style="font-weight:bold;text-align:center;">LoginName</td>
            <td style="font-weight:bold;text-align:center;">CommandText</td>
            <td style="font-weight:bold;text-align:center;">BlockedSessions</td>
          </tr>';

        SELECT @Body += 
            N'<tr>' +
            N'<td>' + CAST(SPID AS NVARCHAR) + '</td>' +
            N'<td>' + ISNULL(JobName, '') + '</td>' +
            N'<td>' + ISNULL(JobStepName, '') + '</td>' +
            N'<td>' + ISNULL(DatabaseName, '') + '</td>' +
            N'<td>' + ISNULL(SessionStatus, '') + '</td>' +
            N'<td>' + ISNULL(LoginName, '') + '</td>' +
            N'<td>' + ISNULL(REPLACE(REPLACE(CommandText, '<', '&lt;'), '>', '&gt;'), '') + '</td>' +
            N'<td>' + ISNULL(BlockedSessions, '') + '</td>' +
            N'</tr>'
        FROM @KillReport;

        SET @Body += N'</table></div>';

        DECLARE @subject NVARCHAR(300) = @@SERVERNAME + ' - Blocking SPIDs Killed - SQL Agent Job Monitor';

        EXEC msdb.dbo.sp_send_dbmail
            @recipients = '<email address>',
            @subject = @subject,
            @body = @Body,
            @body_format = 'HTML';
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


