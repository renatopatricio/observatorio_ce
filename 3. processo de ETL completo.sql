-- =====================================================
-- SCRIPT CORRIGIDO COM PROTEÇÕES IDEMPOTENTES
-- =====================================================

-- =====================================================
-- 1. LIMPEZA PRÉVIA
-- =====================================================
USE msdb;
GO

-- Remover Job se existir
IF EXISTS (SELECT 1 FROM dbo.sysjobs WHERE name = 'COMERCIO_ETL_DAILY')
BEGIN
    EXEC dbo.sp_delete_job @job_name = N'COMERCIO_ETL_DAILY';
    PRINT 'Job COMERCIO_ETL_DAILY removido.';
END
GO

-- Remover Schedule se existir
IF EXISTS (SELECT 1 FROM dbo.sysschedules WHERE name = 'Daily_2AM')
BEGIN
    EXEC dbo.sp_delete_schedule @schedule_name = N'Daily_2AM', @force_delete = 1;
    PRINT 'Schedule Daily_2AM removido.';
END
GO

-- =====================================================
-- 2. ESTRUTURAS STAGING
-- =====================================================
USE COMERCIO_STAGE;
GO

-- Criar schema stg se não existir
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
BEGIN
    EXEC('CREATE SCHEMA stg');
    PRINT 'Schema stg criado.';
END
ELSE
    PRINT 'Schema stg já existe.';
GO

-- Dropar objetos se existirem (para recriar)
IF OBJECT_ID('stg.vw_ETL_Status', 'V') IS NOT NULL DROP VIEW stg.vw_ETL_Status;
IF OBJECT_ID('stg.ETL_LOG', 'U') IS NOT NULL DROP TABLE stg.ETL_LOG;
IF OBJECT_ID('stg.STG_VENDAS', 'U') IS NOT NULL DROP TABLE stg.STG_VENDAS;
IF OBJECT_ID('stg.STG_CLIENTE', 'U') IS NOT NULL DROP TABLE stg.STG_CLIENTE;
IF OBJECT_ID('stg.STG_PRODUTO', 'U') IS NOT NULL DROP TABLE stg.STG_PRODUTO;
IF OBJECT_ID('stg.STG_VENDEDOR', 'U') IS NOT NULL DROP TABLE stg.STG_VENDEDOR;
IF OBJECT_ID('stg.STG_FORNECEDOR', 'U') IS NOT NULL DROP TABLE stg.STG_FORNECEDOR;
IF OBJECT_ID('stg.sp_CleanupETLLogs', 'P') IS NOT NULL DROP PROCEDURE stg.sp_CleanupETLLogs;
GO

-- Tabela de controle ETL
CREATE TABLE stg.ETL_LOG (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT,
    PackageName VARCHAR(100),
    StartTime DATETIME2,
    EndTime DATETIME2,
    Status VARCHAR(20),
    RecordsProcessed INT,
    ErrorMessage VARCHAR(MAX)
);
GO

-- Staging tables
CREATE TABLE stg.STG_CLIENTE (
    COD_CLIENTE INT NOT NULL PRIMARY KEY,
    NOME VARCHAR(200) NULL,
    CPF CHAR(11) NULL,
    EMAIL VARCHAR(200) NULL,
    DT_NASCIMENTO DATE NULL,
    DT_CARGA DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    SOURCE_FILE VARCHAR(255) NULL
);
GO

CREATE TABLE stg.STG_PRODUTO (
    COD_PRODUTO INT NOT NULL PRIMARY KEY,
    DESCRICAO VARCHAR(300) NULL,
    CATEGORIA VARCHAR(100) NULL,
    PRECO DECIMAL(18,2) NULL,
    DT_CARGA DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    SOURCE_FILE VARCHAR(255) NULL
);
GO

CREATE TABLE stg.STG_VENDEDOR (
    COD_VENDEDOR INT NOT NULL PRIMARY KEY,
    NOME VARCHAR(200) NULL,
    REGIAO VARCHAR(100) NULL,
    DT_ADMISSAO DATE NULL,
    DT_CARGA DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    SOURCE_FILE VARCHAR(255) NULL
);
GO

CREATE TABLE stg.STG_FORNECEDOR (
    COD_FORNECEDOR INT NOT NULL PRIMARY KEY,
    NOME_FORNECEDOR VARCHAR(200) NULL,
    CNPJ CHAR(14) NULL,
    CIDADE VARCHAR(100) NULL,
    UF CHAR(2) NULL,
    DT_CARGA DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    SOURCE_FILE VARCHAR(255) NULL
);
GO

CREATE TABLE stg.STG_VENDAS (
    COD_VENDA BIGINT NOT NULL PRIMARY KEY,
    COD_CLIENTE INT NOT NULL,
    COD_PRODUTO INT NOT NULL,
    COD_VENDEDOR INT NOT NULL,
    DT_VENDA DATE NOT NULL,
    QTDE INT NULL,
    VALOR_UNITARIO DECIMAL(18,2) NULL,
    DT_CARGA DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    SOURCE_FILE VARCHAR(255) NULL
);
GO

-- =====================================================
-- 3. ESTRUTURAS DATA WAREHOUSE
-- =====================================================
USE COMERCIO_DW;
GO

-- Criar schemas se não existirem
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')
BEGIN
    EXEC('CREATE SCHEMA dim');
    PRINT 'Schema dim criado.';
END
ELSE
    PRINT 'Schema dim já existe.';
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact')
BEGIN
    EXEC('CREATE SCHEMA fact');
    PRINT 'Schema fact criado.';
END
ELSE
    PRINT 'Schema fact já existe.';
GO

-- Dropar objetos se existirem (para recriar)
IF OBJECT_ID('fact.FATO_VENDAS', 'U') IS NOT NULL 
BEGIN
    ALTER TABLE fact.FATO_VENDAS DROP CONSTRAINT IF EXISTS FK_FATO_DIM_TEMPO;
    ALTER TABLE fact.FATO_VENDAS DROP CONSTRAINT IF EXISTS FK_FATO_DIM_CLIENTE;
    ALTER TABLE fact.FATO_VENDAS DROP CONSTRAINT IF EXISTS FK_FATO_DIM_PRODUTO;
    ALTER TABLE fact.FATO_VENDAS DROP CONSTRAINT IF EXISTS FK_FATO_DIM_VENDEDOR;
    DROP TABLE fact.FATO_VENDAS;
END
GO

IF OBJECT_ID('dim.DIM_CLIENTE', 'U') IS NOT NULL DROP TABLE dim.DIM_CLIENTE;
IF OBJECT_ID('dim.DIM_PRODUTO', 'U') IS NOT NULL DROP TABLE dim.DIM_PRODUTO;
IF OBJECT_ID('dim.DIM_VENDEDOR', 'U') IS NOT NULL DROP TABLE dim.DIM_VENDEDOR;
IF OBJECT_ID('dim.DIM_FORNECEDOR', 'U') IS NOT NULL DROP TABLE dim.DIM_FORNECEDOR;
IF OBJECT_ID('dim.DIM_TEMPO', 'U') IS NOT NULL DROP TABLE dim.DIM_TEMPO;
GO

-- Dropar procedures se existirem
IF OBJECT_ID('dim.sp_SCD_Cliente', 'P') IS NOT NULL DROP PROCEDURE dim.sp_SCD_Cliente;
IF OBJECT_ID('dim.sp_SCD_Produto', 'P') IS NOT NULL DROP PROCEDURE dim.sp_SCD_Produto;
IF OBJECT_ID('dim.sp_SCD_Vendedor', 'P') IS NOT NULL DROP PROCEDURE dim.sp_SCD_Vendedor;
IF OBJECT_ID('dim.sp_SCD_Fornecedor', 'P') IS NOT NULL DROP PROCEDURE dim.sp_SCD_Fornecedor;
IF OBJECT_ID('fact.sp_Load_Fato_Vendas', 'P') IS NOT NULL DROP PROCEDURE fact.sp_Load_Fato_Vendas;
GO

-- DIM_TEMPO
CREATE TABLE dim.DIM_TEMPO (
    DATA_SK INT NOT NULL PRIMARY KEY,
    DATA_COMPLETA DATE NOT NULL,
    ANO SMALLINT NOT NULL,
    TRIMESTRE TINYINT NOT NULL,
    MES TINYINT NOT NULL,
    NOME_MES VARCHAR(20) NOT NULL,
    DIA TINYINT NOT NULL,
    NOME_DIA VARCHAR(20) NOT NULL,
    FIM_DE_SEMANA BIT NOT NULL
);
GO

-- Popular DIM_TEMPO
WITH d AS (
    SELECT TOP (18628)
           DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 1, CAST('20000101' AS DATE)) AS dt
    FROM sys.all_objects
)
INSERT INTO dim.DIM_TEMPO (DATA_SK, DATA_COMPLETA, ANO, TRIMESTRE, MES, NOME_MES, DIA, NOME_DIA, FIM_DE_SEMANA)
SELECT
    CONVERT(INT, FORMAT(dt,'yyyyMMdd')),
    dt,
    DATEPART(YEAR, dt),
    DATEPART(QUARTER, dt),
    DATEPART(MONTH, dt),
    DATENAME(MONTH, dt),
    DATEPART(DAY, dt),
    DATENAME(WEEKDAY, dt),
    CASE WHEN DATENAME(WEEKDAY, dt) IN ('Saturday','Sunday','sábado','domingo') THEN 1 ELSE 0 END
FROM d;
GO

-- DIM_CLIENTE (SCD Tipo 2)
CREATE TABLE dim.DIM_CLIENTE (
    CLIENTE_SK INT IDENTITY(1,1) PRIMARY KEY,
    COD_CLIENTE INT NOT NULL,
    NOME VARCHAR(200) NULL,
    CPF CHAR(11) NULL,
    EMAIL VARCHAR(200) NULL,
    DT_NASCIMENTO DATE NULL,
    DT_INICIO DATE NOT NULL,
    DT_FIM DATE NULL,
    ATIVO BIT NOT NULL,
    HASH_ATTR BINARY(16) NOT NULL
);
GO

CREATE UNIQUE INDEX UX_DIM_CLIENTE_BK ON dim.DIM_CLIENTE (COD_CLIENTE, DT_FIM);
GO

-- DIM_PRODUTO (SCD Tipo 2)
CREATE TABLE dim.DIM_PRODUTO (
    PRODUTO_SK INT IDENTITY(1,1) PRIMARY KEY,
    COD_PRODUTO INT NOT NULL,
    DESCRICAO VARCHAR(300) NULL,
    CATEGORIA VARCHAR(100) NULL,
    PRECO_ATUAL DECIMAL(18,2) NULL,
    DT_INICIO DATE NOT NULL,
    DT_FIM DATE NULL,
    ATIVO BIT NOT NULL,
    HASH_ATTR BINARY(16) NOT NULL
);
GO

CREATE UNIQUE INDEX UX_DIM_PRODUTO_BK ON dim.DIM_PRODUTO (COD_PRODUTO, DT_FIM);
GO

-- DIM_VENDEDOR (SCD Tipo 2)
CREATE TABLE dim.DIM_VENDEDOR (
    VENDEDOR_SK INT IDENTITY(1,1) PRIMARY KEY,
    COD_VENDEDOR INT NOT NULL,
    NOME VARCHAR(200) NULL,
    REGIAO VARCHAR(100) NULL,
    DT_ADMISSAO DATE NULL,
    DT_INICIO DATE NOT NULL,
    DT_FIM DATE NULL,
    ATIVO BIT NOT NULL,
    HASH_ATTR BINARY(16) NOT NULL
);
GO

CREATE UNIQUE INDEX UX_DIM_VENDEDOR_BK ON dim.DIM_VENDEDOR (COD_VENDEDOR, DT_FIM);
GO

-- DIM_FORNECEDOR (SCD Tipo 2)
CREATE TABLE dim.DIM_FORNECEDOR (
    FORNECEDOR_SK INT IDENTITY(1,1) PRIMARY KEY,
    COD_FORNECEDOR INT NOT NULL,
    NOME_FORNECEDOR VARCHAR(200) NULL,
    CNPJ CHAR(14) NULL,
    CIDADE VARCHAR(100) NULL,
    UF CHAR(2) NULL,
    DT_INICIO DATE NOT NULL,
    DT_FIM DATE NULL,
    ATIVO BIT NOT NULL,
    HASH_ATTR BINARY(16) NOT NULL
);
GO

CREATE UNIQUE INDEX UX_DIM_FORNECEDOR_BK ON dim.DIM_FORNECEDOR (COD_FORNECEDOR, DT_FIM);
GO

-- FATO_VENDAS
CREATE TABLE fact.FATO_VENDAS (
    FATO_ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    DATA_SK INT NOT NULL,
    CLIENTE_SK INT NOT NULL,
    PRODUTO_SK INT NOT NULL,
    VENDEDOR_SK INT NOT NULL,
    QTDE INT NOT NULL,
    VALOR_UNITARIO DECIMAL(18,2) NOT NULL,
    VL_TOTAL AS (QTDE * VALOR_UNITARIO) PERSISTED,
    COD_VENDA_ORIGEM BIGINT NOT NULL,
    CONSTRAINT UX_FATO_VENDAS UNIQUE (COD_VENDA_ORIGEM)
);
GO

-- Foreign Keys
ALTER TABLE fact.FATO_VENDAS WITH CHECK
    ADD CONSTRAINT FK_FATO_DIM_TEMPO FOREIGN KEY (DATA_SK) REFERENCES dim.DIM_TEMPO(DATA_SK),
        CONSTRAINT FK_FATO_DIM_CLIENTE FOREIGN KEY (CLIENTE_SK) REFERENCES dim.DIM_CLIENTE(CLIENTE_SK),
        CONSTRAINT FK_FATO_DIM_PRODUTO FOREIGN KEY (PRODUTO_SK) REFERENCES dim.DIM_PRODUTO(PRODUTO_SK),
        CONSTRAINT FK_FATO_DIM_VENDEDOR FOREIGN KEY (VENDEDOR_SK) REFERENCES dim.DIM_VENDEDOR(VENDEDOR_SK);
GO

-- Índices de performance
CREATE INDEX IX_FATO_VENDAS_DATA ON fact.FATO_VENDAS (DATA_SK);
CREATE INDEX IX_FATO_VENDAS_CLIENTE ON fact.FATO_VENDAS (CLIENTE_SK);
CREATE INDEX IX_FATO_VENDAS_PRODUTO ON fact.FATO_VENDAS (PRODUTO_SK);
CREATE INDEX IX_FATO_VENDAS_VENDEDOR ON fact.FATO_VENDAS (VENDEDOR_SK);
GO

-- =====================================================
-- 4. PROCEDURES ETL
-- =====================================================

-- Procedure SCD Tipo 2 para CLIENTE
CREATE PROCEDURE dim.sp_SCD_Cliente
AS
BEGIN
    SET NOCOUNT ON;
    
    WITH src AS (
        SELECT 
            c.COD_CLIENTE,
            c.NOME,
            c.CPF,
            c.EMAIL,
            c.DT_NASCIMENTO,
            CONVERT(BINARY(16), HASHBYTES('MD5', 
                 CONCAT(ISNULL(c.NOME,''),'|',ISNULL(c.CPF,''),'|',ISNULL(c.EMAIL,''),'|',ISNULL(CAST(c.DT_NASCIMENTO AS VARCHAR),'')))) AS HASH_ATTR
        FROM COMERCIO_STAGE.stg.STG_CLIENTE c
    )
    MERGE dim.DIM_CLIENTE AS tgt
    USING src ON tgt.COD_CLIENTE = src.COD_CLIENTE AND tgt.ATIVO = 1
    WHEN MATCHED AND tgt.HASH_ATTR <> src.HASH_ATTR THEN
        UPDATE SET tgt.ATIVO = 0, tgt.DT_FIM = CAST(GETDATE() AS DATE)
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (COD_CLIENTE, NOME, CPF, EMAIL, DT_NASCIMENTO, DT_INICIO, ATIVO, HASH_ATTR)
        VALUES (src.COD_CLIENTE, src.NOME, src.CPF, src.EMAIL, src.DT_NASCIMENTO,
                CAST(GETDATE() AS DATE), 1, src.HASH_ATTR);

    -- Inserir nova versão para registros alterados
    INSERT INTO dim.DIM_CLIENTE (COD_CLIENTE, NOME, CPF, EMAIL, DT_NASCIMENTO, DT_INICIO, ATIVO, HASH_ATTR)
    SELECT s.COD_CLIENTE, s.NOME, s.CPF, s.EMAIL, s.DT_NASCIMENTO,
           CAST(GETDATE() AS DATE), 1, s.HASH_ATTR
    FROM src s
    WHERE EXISTS (
        SELECT 1 FROM dim.DIM_CLIENTE d
        WHERE d.COD_CLIENTE = s.COD_CLIENTE
          AND d.ATIVO = 0
          AND d.DT_FIM = CAST(GETDATE() AS DATE)
    );
END;
GO

-- Procedure SCD Tipo 2 para PRODUTO
CREATE PROCEDURE dim.sp_SCD_Produto
AS
BEGIN
    SET NOCOUNT ON;
    
    WITH src AS (
        SELECT 
            p.COD_PRODUTO,
            p.DESCRICAO,
            p.CATEGORIA,
            p.PRECO,
            CONVERT(BINARY(16), HASHBYTES('MD5', 
                 CONCAT(ISNULL(p.DESCRICAO,''),'|',ISNULL(p.CATEGORIA,''),'|',ISNULL(CAST(p.PRECO AS VARCHAR),'')))) AS HASH_ATTR
        FROM COMERCIO_STAGE.stg.STG_PRODUTO p
    )
    MERGE dim.DIM_PRODUTO AS tgt
    USING src ON tgt.COD_PRODUTO = src.COD_PRODUTO AND tgt.ATIVO = 1
    WHEN MATCHED AND tgt.HASH_ATTR <> src.HASH_ATTR THEN
        UPDATE SET tgt.ATIVO = 0, tgt.DT_FIM = CAST(GETDATE() AS DATE)
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (COD_PRODUTO, DESCRICAO, CATEGORIA, PRECO_ATUAL, DT_INICIO, ATIVO, HASH_ATTR)
        VALUES (src.COD_PRODUTO, src.DESCRICAO, src.CATEGORIA, src.PRECO,
                CAST(GETDATE() AS DATE), 1, src.HASH_ATTR);

    INSERT INTO dim.DIM_PRODUTO (COD_PRODUTO, DESCRICAO, CATEGORIA, PRECO_ATUAL, DT_INICIO, ATIVO, HASH_ATTR)
    SELECT s.COD_PRODUTO, s.DESCRICAO, s.CATEGORIA, s.PRECO,
           CAST(GETDATE() AS DATE), 1, s.HASH_ATTR
    FROM src s
    WHERE EXISTS (
        SELECT 1 FROM dim.DIM_PRODUTO d
        WHERE d.COD_PRODUTO = s.COD_PRODUTO
          AND d.ATIVO = 0
          AND d.DT_FIM = CAST(GETDATE() AS DATE)
    );
END;
GO

-- Procedure SCD Tipo 2 para VENDEDOR
CREATE PROCEDURE dim.sp_SCD_Vendedor
AS
BEGIN
    SET NOCOUNT ON;
    
    WITH src AS (
        SELECT 
            v.COD_VENDEDOR,
            v.NOME,
            v.REGIAO,
            v.DT_ADMISSAO,
            CONVERT(BINARY(16), HASHBYTES('MD5', 
                 CONCAT(ISNULL(v.NOME,''),'|',ISNULL(v.REGIAO,''),'|',ISNULL(CAST(v.DT_ADMISSAO AS VARCHAR),'')))) AS HASH_ATTR
        FROM COMERCIO_STAGE.stg.STG_VENDEDOR v
    )
    MERGE dim.DIM_VENDEDOR AS tgt
    USING src ON tgt.COD_VENDEDOR = src.COD_VENDEDOR AND tgt.ATIVO = 1
    WHEN MATCHED AND tgt.HASH_ATTR <> src.HASH_ATTR THEN
        UPDATE SET tgt.ATIVO = 0, tgt.DT_FIM = CAST(GETDATE() AS DATE)
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (COD_VENDEDOR, NOME, REGIAO, DT_ADMISSAO, DT_INICIO, ATIVO, HASH_ATTR)
        VALUES (src.COD_VENDEDOR, src.NOME, src.REGIAO, src.DT_ADMISSAO,
                CAST(GETDATE() AS DATE), 1, src.HASH_ATTR);

    INSERT INTO dim.DIM_VENDEDOR (COD_VENDEDOR, NOME, REGIAO, DT_ADMISSAO, DT_INICIO, ATIVO, HASH_ATTR)
    SELECT s.COD_VENDEDOR, s.NOME, s.REGIAO, s.DT_ADMISSAO,
           CAST(GETDATE() AS DATE), 1, s.HASH_ATTR
    FROM src s
    WHERE EXISTS (
        SELECT 1 FROM dim.DIM_VENDEDOR d
        WHERE d.COD_VENDEDOR = s.COD_VENDEDOR
          AND d.ATIVO = 0
          AND d.DT_FIM = CAST(GETDATE() AS DATE)
    );
END;
GO

-- Procedure SCD Tipo 2 para FORNECEDOR
CREATE PROCEDURE dim.sp_SCD_Fornecedor
AS
BEGIN
    SET NOCOUNT ON;
    
    WITH src AS (
        SELECT 
            f.COD_FORNECEDOR,
            f.NOME_FORNECEDOR,
            f.CNPJ,
            f.CIDADE,
            f.UF,
            CONVERT(BINARY(16), HASHBYTES('MD5', 
                 CONCAT(ISNULL(f.NOME_FORNECEDOR,''),'|',ISNULL(f.CNPJ,''),'|',ISNULL(f.CIDADE,''),'|',ISNULL(f.UF,'')))) AS HASH_ATTR
        FROM COMERCIO_STAGE.stg.STG_FORNECEDOR f
    )
    MERGE dim.DIM_FORNECEDOR AS tgt
    USING src ON tgt.COD_FORNECEDOR = src.COD_FORNECEDOR AND tgt.ATIVO = 1
    WHEN MATCHED AND tgt.HASH_ATTR <> src.HASH_ATTR THEN
        UPDATE SET tgt.ATIVO = 0, tgt.DT_FIM = CAST(GETDATE() AS DATE)
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (COD_FORNECEDOR, NOME_FORNECEDOR, CNPJ, CIDADE, UF, DT_INICIO, ATIVO, HASH_ATTR)
        VALUES (src.COD_FORNECEDOR, src.NOME_FORNECEDOR, src.CNPJ, src.CIDADE, src.UF,
                CAST(GETDATE() AS DATE), 1, src.HASH_ATTR);

    INSERT INTO dim.DIM_FORNECEDOR (COD_FORNECEDOR, NOME_FORNECEDOR, CNPJ, CIDADE, UF, DT_INICIO, ATIVO, HASH_ATTR)
    SELECT s.COD_FORNECEDOR, s.NOME_FORNECEDOR, s.CNPJ, s.CIDADE, s.UF,
           CAST(GETDATE() AS DATE), 1, s.HASH_ATTR
    FROM src s
    WHERE EXISTS (
        SELECT 1 FROM dim.DIM_FORNECEDOR d
        WHERE d.COD_FORNECEDOR = s.COD_FORNECEDOR
          AND d.ATIVO = 0
          AND d.DT_FIM = CAST(GETDATE() AS DATE)
    );
END;
GO

-- Procedure para carregar FATO_VENDAS
CREATE PROCEDURE fact.sp_Load_Fato_Vendas
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO fact.FATO_VENDAS (DATA_SK, CLIENTE_SK, PRODUTO_SK, VENDEDOR_SK, QTDE, VALOR_UNITARIO, COD_VENDA_ORIGEM)
    SELECT
        CONVERT(INT, FORMAT(v.DT_VENDA,'yyyyMMdd')) AS DATA_SK,
        c.CLIENTE_SK,
        p.PRODUTO_SK,
        vnd.VENDEDOR_SK,
        v.QTDE,
        v.VALOR_UNITARIO,
        v.COD_VENDA
    FROM COMERCIO_STAGE.stg.STG_VENDAS v
    JOIN dim.DIM_CLIENTE c ON c.COD_CLIENTE = v.COD_CLIENTE AND c.ATIVO = 1
    JOIN dim.DIM_PRODUTO p ON p.COD_PRODUTO = v.COD_PRODUTO AND p.ATIVO = 1
    JOIN dim.DIM_VENDEDOR vnd ON vnd.COD_VENDEDOR = v.COD_VENDEDOR AND vnd.ATIVO = 1
    WHERE NOT EXISTS (
        SELECT 1 FROM fact.FATO_VENDAS f
        WHERE f.COD_VENDA_ORIGEM = v.COD_VENDA
    );
END;
GO

-- =====================================================
-- 5. PROCEDURES DE MONITORAMENTO
-- =====================================================
USE COMERCIO_STAGE;
GO

-- Procedure de limpeza de logs
CREATE PROCEDURE stg.sp_CleanupETLLogs
    @RetentionDays INT = 30
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @DeletedRows INT;
    
    DELETE FROM stg.ETL_LOG 
    WHERE StartTime < DATEADD(DAY, -@RetentionDays, GETDATE());
    
    SET @DeletedRows = @@ROWCOUNT;
    
    PRINT 'Logs antigos removidos: ' + CAST(@DeletedRows AS VARCHAR) + ' registros. Retenção: ' + CAST(@RetentionDays AS VARCHAR) + ' dias.';
END;
GO

-- View para monitoramento
CREATE VIEW stg.vw_ETL_Status
AS
SELECT 
    BatchID,
    PackageName,
    StartTime,
    EndTime,
    Status,
    RecordsProcessed,
    DATEDIFF(MINUTE, StartTime, ISNULL(EndTime, GETDATE())) AS DurationMinutes,
    CASE 
        WHEN Status = 'RUNNING' AND DATEDIFF(MINUTE, StartTime, GETDATE()) > 60 
        THEN 'ALERT - Long Running'
        ELSE Status 
    END AS StatusAlert
FROM stg.ETL_LOG;
GO

-- =====================================================
-- 6. SQL SERVER AGENT JOB
-- =====================================================
USE msdb;
GO

-- Criar Job
EXEC dbo.sp_add_job
    @job_name = N'COMERCIO_ETL_DAILY',
    @enabled = 1,
    @description = N'ETL diário para Data Warehouse Comercio';
GO

-- Step 1: Limpar Staging
EXEC dbo.sp_add_jobstep
    @job_name = N'COMERCIO_ETL_DAILY',
    @step_name = N'01_Clear_Staging',
    @subsystem = N'TSQL',
    @database_name = N'COMERCIO_STAGE',
    @command = N'
    TRUNCATE TABLE stg.STG_CLIENTE;
    TRUNCATE TABLE stg.STG_PRODUTO;
    TRUNCATE TABLE stg.STG_VENDEDOR;
    TRUNCATE TABLE stg.STG_FORNECEDOR;
    TRUNCATE TABLE stg.STG_VENDAS;

    INSERT INTO stg.ETL_LOG (BatchID, PackageName, StartTime, Status)
    VALUES (CONVERT(INT, FORMAT(GETDATE(), ''yyyyMMddHH'')), ''COMERCIO_ETL_DAILY'', GETDATE(), ''RUNNING'');
    
    PRINT ''Staging limpo e log iniciado.'';
    ',
    @on_success_action = 3,
    @on_fail_action = 2;
GO

-- Step 2: Load Staging - Clientes (com dados de teste)
EXEC dbo.sp_add_jobstep
    @job_name = N'COMERCIO_ETL_DAILY',
    @step_name = N'02_Load_Stage_Clientes',
    @subsystem = N'TSQL',
    @database_name = N'COMERCIO_STAGE',
    @command = N'
    -- Dados de teste (substitua pela sua fonte real)
    INSERT INTO stg.STG_CLIENTE (COD_CLIENTE, NOME, CPF, EMAIL, DT_NASCIMENTO, SOURCE_FILE)
    VALUES 
        (1, ''JOÃO SILVA'', ''12345678901'', ''joao@email.com'', ''1980-01-15'', ''OLTP_CLIENTE''),
        (2, ''MARIA SANTOS'', ''98765432100'', ''maria@email.com'', ''1975-05-20'', ''OLTP_CLIENTE''),
        (3, ''PEDRO OLIVEIRA'', ''11122233344'', ''pedro@email.com'', ''1990-12-10'', ''OLTP_CLIENTE'');
    
    PRINT ''Clientes carregados: '' + CAST(@@ROWCOUNT AS VARCHAR);
    ',
    @on_success_action = 3,
    @on_fail_action = 2;
GO

-- Step 3: Load Staging - Produtos
EXEC dbo.sp_add_jobstep
    @job_name = N'COMERCIO_ETL_DAILY',
    @step_name = N'03_Load_Stage_Produtos',
    @subsystem = N'TSQL',
    @database_name = N'COMERCIO_STAGE',
    @command = N'
    INSERT INTO stg.STG_PRODUTO (COD_PRODUTO, DESCRICAO, CATEGORIA, PRECO, SOURCE_FILE)
    VALUES 
        (1, ''NOTEBOOK DELL'', ''INFORMATICA'', 2500.00, ''OLTP_PRODUTO''),
        (2, ''MOUSE LOGITECH'', ''INFORMATICA'', 50.00, ''OLTP_PRODUTO''),
        (3, ''TECLADO MECANICO'', ''INFORMATICA'', 150.00, ''OLTP_PRODUTO'');
    
    PRINT ''Produtos carregados: '' + CAST(@@ROWCOUNT AS VARCHAR);
    ',
    @on_success_action = 3,
    @on_fail_action = 2;
GO

-- Step 4: Load Staging - Vendedores
EXEC dbo.sp_add_jobstep
    @job_name = N'COMERCIO_ETL_DAILY',
    @step_name = N'04_Load_Stage_Vendedores',
    @subsystem = N'TSQL',
    @database_name = N'COMERCIO_STAGE',
    @command = N'
    INSERT INTO stg.STG_VENDEDOR (COD_VENDEDOR, NOME, REGIAO, DT_ADMISSAO, SOURCE_FILE)
    VALUES 
        (1, ''CARLOS VENDAS'', ''SUL'', ''2020-01-01'', ''OLTP_VENDEDOR''),
        (2, ''ANA COMERCIAL'', ''SUDESTE'', ''2019-06-15'', ''OLTP_VENDEDOR''),
        (3, ''LUIS REPRESENTANTE'', ''NORDESTE'', ''2021-03-10'', ''OLTP_VENDEDOR'');
    
    PRINT ''Vendedores carregados: '' + CAST(@@ROWCOUNT AS VARCHAR);
    ',
    @on_success_action = 3,
    @on_fail_action = 2;
GO

-- Step 5: Load Staging - Fornecedores
EXEC dbo.sp_add_jobstep
    @job_name = N'COMERCIO_ETL_DAILY',
    @step_name = N'05_Load_Stage_Fornecedores',
    @subsystem = N'TSQL',
    @database_name = N'COMERCIO_STAGE',
    @command = N'
    INSERT INTO stg.STG_FORNECEDOR (COD_FORNECEDOR, NOME_FORNECEDOR, CNPJ, CIDADE, UF, SOURCE_FILE)
    VALUES 
        (1, ''DELL COMPUTADORES'', ''12345678000199'', ''SAO PAULO'', ''SP'', ''OLTP_FORNECEDOR''),
        (2, ''LOGITECH BRASIL'', ''98765432000188'', ''RIO DE JANEIRO'', ''RJ'', ''OLTP_FORNECEDOR'');
    
    PRINT ''Fornecedores carregados: '' + CAST(@@ROWCOUNT AS VARCHAR);
    ',
    @on_success_action = 3,
    @on_fail_action = 2;
GO

-- Step 6: Load Staging - Vendas
EXEC dbo.sp_add_jobstep
    @job_name = N'COMERCIO_ETL_DAILY',
    @step_name = N'06_Load_Stage_Vendas',
    @subsystem = N'TSQL',
    @database_name = N'COMERCIO_STAGE',
    @command = N'
    INSERT INTO stg.STG_VENDAS (COD_VENDA, COD_CLIENTE, COD_PRODUTO, COD_VENDEDOR, DT_VENDA, QTDE, VALOR_UNITARIO, SOURCE_FILE)
    VALUES 
        (1, 1, 1, 1, GETDATE()-1, 1, 2500.00, ''OLTP_VENDAS''),
        (2, 2, 2, 2, GETDATE()-1, 2, 50.00, ''OLTP_VENDAS''),
        (3, 3, 3, 3, GETDATE(), 1, 150.00, ''OLTP_VENDAS''),
        (4, 1, 2, 1, GETDATE(), 1, 50.00, ''OLTP_VENDAS'');
    
    PRINT ''Vendas carregadas: '' + CAST(@@ROWCOUNT AS VARCHAR);
    ',
    @on_success_action = 3,
    @on_fail_action = 2;
GO

-- Step 7: Load DW - Dimensões
EXEC dbo.sp_add_jobstep
    @job_name = N'COMERCIO_ETL_DAILY',
    @step_name = N'07_Load_DW_Dimensions',
    @subsystem = N'TSQL',
    @database_name = N'COMERCIO_DW',
    @command = N'
    EXEC dim.sp_SCD_Cliente;
    EXEC dim.sp_SCD_Produto;
    EXEC dim.sp_SCD_Vendedor;
    EXEC dim.sp_SCD_Fornecedor;
    
    PRINT ''Dimensões processadas.'';
    ',
    @on_success_action = 3,
    @on_fail_action = 2;
GO

-- Step 8: Load DW - Fato
EXEC dbo.sp_add_jobstep
    @job_name = N'COMERCIO_ETL_DAILY',
    @step_name = N'08_Load_DW_Facts',
    @subsystem = N'TSQL',
    @database_name = N'COMERCIO_DW',
    @command = N'
    EXEC fact.sp_Load_Fato_Vendas;
    
    PRINT ''Fatos processados.'';
    ',
    @on_success_action = 3,
    @on_fail_action = 2;
GO

-- Step 9: Validação Final
EXEC dbo.sp_add_jobstep
    @job_name = N'COMERCIO_ETL_DAILY',
    @step_name = N'09_Validation',
    @subsystem = N'TSQL',
    @database_name = N'COMERCIO_DW',
    @command = N'
    DECLARE @ClienteCount INT, @VendasCount INT;

    SELECT @ClienteCount = COUNT(*) FROM dim.DIM_CLIENTE WHERE ATIVO = 1;
    SELECT @VendasCount = COUNT(*) FROM fact.FATO_VENDAS;

    UPDATE COMERCIO_STAGE.stg.ETL_LOG 
    SET EndTime = GETDATE(), 
        Status = ''SUCCESS'',
        RecordsProcessed = @VendasCount
    WHERE BatchID = CONVERT(INT, FORMAT(GETDATE(), ''yyyyMMddHH''))
      AND PackageName = ''COMERCIO_ETL_DAILY''
      AND EndTime IS NULL;

    PRINT ''ETL completed successfully'';
    PRINT ''Clientes ativos: '' + CAST(@ClienteCount AS VARCHAR);
    PRINT ''Vendas carregadas: '' + CAST(@VendasCount AS VARCHAR);
    ',
    @on_success_action = 1,
    @on_fail_action = 2;
GO

-- Configurar schedule (diário às 02:00)
EXEC dbo.sp_add_schedule
    @schedule_name = N'Daily_2AM',
    @freq_type = 4,
    @freq_interval = 1,
    @active_start_time = 020000;
GO

EXEC dbo.sp_attach_schedule
    @job_name = N'COMERCIO_ETL_DAILY',
    @schedule_name = N'Daily_2AM';
GO

-- Adicionar job ao servidor
EXEC dbo.sp_add_jobserver
    @job_name = N'COMERCIO_ETL_DAILY';
GO

-- =====================================================
-- FINALIZAÇÃO
-- =====================================================
PRINT '✅ SCRIPT EXECUTADO COM SUCESSO!';
PRINT '';
PRINT '🔧 OBJETOS CRIADOS:';
PRINT '   - Schemas: stg, dim, fact';
PRINT '   - Tabelas: Staging + Dimensões + Fato';
PRINT '   - Procedures: SCD Tipo 2 + Carga de Fatos';
PRINT '   - Job: COMERCIO_ETL_DAILY (agendado para 02:00)';
PRINT '';
PRINT '🚀 PARA TESTAR MANUALMENTE:';
PRINT '   EXEC msdb.dbo.sp_start_job @job_name = ''COMERCIO_ETL_DAILY'';';
PRINT '';
PRINT '📊 PARA MONITORAR:';
PRINT '   SELECT * FROM COMERCIO_STAGE.stg.vw_ETL_Status;';
GO