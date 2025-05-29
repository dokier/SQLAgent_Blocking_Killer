USE [NSP_TEMP]
GO

/****** Object:  Table [dbo].[Agent_Job_Killable_Whitelist]    Script Date: 5/29/2025 4:08:59 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Agent_Job_Killable_Whitelist](
	[JobName] [sysname] NOT NULL,
	[IsActive] [bit] NOT NULL,
	[AddedDate] [datetime] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[JobName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Agent_Job_Killable_Whitelist] ADD  DEFAULT ((1)) FOR [IsActive]
GO

ALTER TABLE [dbo].[Agent_Job_Killable_Whitelist] ADD  DEFAULT (getdate()) FOR [AddedDate]
GO


