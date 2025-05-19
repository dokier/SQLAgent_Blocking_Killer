USE [NSP_TEMP]
GO

/****** Object:  Table [dbo].[Agent_Job_Blocking_Kill_Log]    Script Date: 5/19/2025 2:29:25 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Agent_Job_Blocking_Kill_Log](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[KillDate] [datetime2](3) NOT NULL,
	[SPID] [int] NOT NULL,
	[ProgramName] [nvarchar](255) NOT NULL,
	[JobName] [sysname] NULL,
	[JobStepName] [nvarchar](255) NULL,
	[KilledBy] [sysname] NOT NULL,
	[DatabaseName] [nvarchar](128) NULL,
	[SessionStatus] [nvarchar](60) NULL,
	[LoginName] [nvarchar](128) NULL,
	[CommandText] [nvarchar](max) NULL,
	[BlockedSessions] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[Agent_Job_Blocking_Kill_Log] ADD  CONSTRAINT [DF_KillDate]  DEFAULT (sysdatetime()) FOR [KillDate]
GO

ALTER TABLE [dbo].[Agent_Job_Blocking_Kill_Log] ADD  CONSTRAINT [DF_KilledBy]  DEFAULT (original_login()) FOR [KilledBy]
GO


