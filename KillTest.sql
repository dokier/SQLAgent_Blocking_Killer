USE [NSP_TEMP]
GO

/****** Object:  Table [dbo].[KillTest]    Script Date: 5/20/2025 9:54:06 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[KillTest](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Data] [char](2000) NULL
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[KillTest] ADD  DEFAULT ('x') FOR [Data]
GO


