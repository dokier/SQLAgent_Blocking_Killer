USE [NSP_TEMP]
GO

/****** Object:  StoredProcedure [dbo].[Log_Error_Message]    Script Date: 5/19/2025 2:28:47 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE   PROCEDURE [dbo].[Log_Error_Message]
AS
BEGIN TRY

    SET NOCOUNT ON;

    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    INSERT INTO [dbo].[ErrorLog]
    (
        [MessageId],
        [MessageText],
        [SeverityLevel],
        [State],
        [LineNumber],
        [ProcedureName],
		[DatabaseName]
    )
    VALUES
    (ERROR_NUMBER(), ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_LINE(), ERROR_PROCEDURE(), DB_NAME());

    COMMIT TRANSACTION;

END TRY
BEGIN CATCH

    IF (@@TRANCOUNT > 0)
        ROLLBACK TRANSACTION;

    THROW;

END CATCH;
GO


