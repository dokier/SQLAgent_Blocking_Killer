USE [NSP_TEMP]
GO

/****** Object:  Table [dbo].[ErrorLog]    Script Date: 5/19/2025 2:30:12 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ErrorLog](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[MessageId] [int] NOT NULL,
	[MessageText] [nvarchar](2047) NULL,
	[SeverityLevel] [int] NOT NULL,
	[State] [int] NOT NULL,
	[LineNumber] [int] NOT NULL,
	[ProcedureName] [nvarchar](2500) NULL,
	[DatabaseName] [nvarchar](128) NULL,
	[CreateDate] [datetime] NOT NULL,
 CONSTRAINT [PK_ErrorLog_Id] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ErrorLog] ADD  CONSTRAINT [DF__ErrorLog__Create__5629CD9C]  DEFAULT (getdate()) FOR [CreateDate]
GO


