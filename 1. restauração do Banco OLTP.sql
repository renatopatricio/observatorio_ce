-- Ajuste os caminhos do .BAK, .mdf e .ldf ao seu ambiente
USE master;
GO

RESTORE FILELISTONLY 
FROM DISK = N'C:\Backups\COMERCIO_OLTP.BAK';
GO
-- Substitua abaixo os nomes lógicos retornados (LogicalName)

RESTORE DATABASE [COMERCIO_OLTP]
FROM DISK = N'C:\Backups\COMERCIO_OLTP.BAK'
WITH 
    MOVE N'LogicalDataName' TO N'C:\SQLData\COMERCIO_OLTP.mdf',
    MOVE N'LogicalLogName'  TO N'C:\SQLLogs\COMERCIO_OLTP_log.ldf',
    REPLACE,
    RECOVERY,
    STATS = 10;
GO