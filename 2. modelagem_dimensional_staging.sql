
/* 2 – Modelagem e Estruturação da Solução de Dados */
/* 1.1  Schema dedicado */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'stg')
    EXEC('CREATE SCHEMA stg');
GO

/* 1.2  Tabelas de staging  (tipos padronizados, nulos tratados) */
-- CLIENTES -------------------------------------------------------------------
IF OBJECT_ID('stg.STG_CLIENTE','U') IS NOT NULL
    DROP TABLE stg.STG_CLIENTE;
CREATE TABLE stg.STG_CLIENTE (
    COD_CLIENTE        INT            NOT NULL,
    NOME               VARCHAR(200)   NULL,
    CPF                CHAR(11)       NULL,
    EMAIL              VARCHAR(200)   NULL,
    DT_NASCIMENTO      DATE           NULL,
    DT_CARGA           DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    SOURCE_FILE        VARCHAR(255)   NULL,
    CONSTRAINT PK_STG_CLIENTE PRIMARY KEY (COD_CLIENTE)
);

-- PRODUTOS -------------------------------------------------------------------
IF OBJECT_ID('stg.STG_PRODUTO','U') IS NOT NULL
    DROP TABLE stg.STG_PRODUTO;
CREATE TABLE stg.STG_PRODUTO (
    COD_PRODUTO        INT            NOT NULL,
    DESCRICAO          VARCHAR(300)   NULL,
    CATEGORIA          VARCHAR(100)   NULL,
    PRECO              DECIMAL(18,2)  NULL,
    DT_CARGA           DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    SOURCE_FILE        VARCHAR(255)   NULL,
    CONSTRAINT PK_STG_PRODUTO PRIMARY KEY (COD_PRODUTO)
);

-- VENDEDORES -----------------------------------------------------------------
IF OBJECT_ID('stg.STG_VENDEDOR','U') IS NOT NULL
    DROP TABLE stg.STG_VENDEDOR;
CREATE TABLE stg.STG_VENDEDOR (
    COD_VENDEDOR       INT            NOT NULL,
    NOME               VARCHAR(200)   NULL,
    REGIAO             VARCHAR(100)   NULL,
    DT_ADMISSAO        DATE           NULL,
    DT_CARGA           DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    SOURCE_FILE        VARCHAR(255)   NULL,
    CONSTRAINT PK_STG_VENDEDOR PRIMARY KEY (COD_VENDEDOR)
);
-- VENDAS ---------------------------------------------------------------------
IF OBJECT_ID('stg.STG_VENDAS','U') IS NOT NULL
    DROP TABLE stg.STG_VENDAS;
CREATE TABLE stg.STG_VENDAS (
    COD_VENDA          BIGINT         NOT NULL,
    COD_CLIENTE        INT            NOT NULL,
    COD_PRODUTO        INT            NOT NULL,
    COD_VENDEDOR       INT            NOT NULL,
    DT_VENDA           DATE           NOT NULL,
    QTDE               INT            NULL,
    VALOR_UNITARIO     DECIMAL(18,2)  NULL,
    DT_CARGA           DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    SOURCE_FILE        VARCHAR(255)   NULL,
    CONSTRAINT PK_STG_VENDAS PRIMARY KEY (COD_VENDA)
);

/* =========================================================
   2.   DATA WAREHOUSE – Star Schema
   ========================================================= */
USE COMERCIO_DW;
GO

/* 2.1  Schemas */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dim')
    EXEC('CREATE SCHEMA dim');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'fact')
    EXEC('CREATE SCHEMA fact');
GO
/* 2.2  Dimensão TEMPO  (já populada para 2000-01-01 a 2050-12-31) */
IF OBJECT_ID('dim.DIM_TEMPO','U') IS NOT NULL
    DROP TABLE dim.DIM_TEMPO;
CREATE TABLE dim.DIM_TEMPO (
    DATA_SK          INT           NOT NULL PRIMARY KEY,      -- yyyyMMdd
    DATA_COMPLETA    DATE          NOT NULL,
    ANO              SMALLINT      NOT NULL,
    TRIMESTRE        TINYINT       NOT NULL,
    MES              TINYINT       NOT NULL,
    NOME_MES         VARCHAR(20)   NOT NULL,
    DIA              TINYINT       NOT NULL,
    NOME_DIA         VARCHAR(20)   NOT NULL,
    FIM_DE_SEMANA    BIT           NOT NULL
);

-- População idempotente

WITH d AS (
    SELECT TOP (18628)            -- ~51 anos
           DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 1, CAST('20000101' AS DATE)) AS dt
    FROM sys.all_objects
)
INSERT INTO dim.DIM_TEMPO (DATA_SK, DATA_COMPLETA, ANO, TRIMESTRE, MES, NOME_MES,
                           DIA, NOME_DIA, FIM_DE_SEMANA)
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
FROM d
WHERE NOT EXISTS (SELECT 1 FROM dim.DIM_TEMPO WHERE DATA_SK = CONVERT(INT, FORMAT(dt,'yyyyMMdd')));
GO

/* 2.3  Dimensão CLIENTE  – SCD Tipo 2  */
IF OBJECT_ID('dim.DIM_CLIENTE','U') IS NOT NULL
    DROP TABLE dim.DIM_CLIENTE;
CREATE TABLE dim.DIM_CLIENTE (
    CLIENTE_SK        INT           IDENTITY(1,1)  PRIMARY KEY,
    COD_CLIENTE       INT           NOT NULL,                         -- business key
    NOME              VARCHAR(200)  NULL,
    CPF               CHAR(11)      NULL,
    EMAIL             VARCHAR(200)  NULL,
    DT_NASCIMENTO     DATE          NULL,
    DT_INICIO         DATE          NOT NULL,                         -- SCD2
    DT_FIM            DATE          NULL,                             -- SCD2
    ATIVO             BIT           NOT NULL,
    HASH_ATTR         BINARY(16)    NOT NULL                          -- p/ detecção de mudanças
);
CREATE UNIQUE INDEX UX_DIM_CLIENTE_BK ON dim.DIM_CLIENTE (COD_CLIENTE, DT_FIM);

/* 2.4  Dimensão PRODUTO  (Tipo 1) */
IF OBJECT_ID('dim.DIM_PRODUTO','U') IS NOT NULL
    DROP TABLE dim.DIM_PRODUTO;
CREATE TABLE dim.DIM_PRODUTO (
    PRODUTO_SK        INT           IDENTITY(1,1)  PRIMARY KEY,
    COD_PRODUTO       INT           NOT NULL UNIQUE,
    DESCRICAO         VARCHAR(300)  NULL,
    CATEGORIA         VARCHAR(100)  NULL,
    PRECO_ATUAL       DECIMAL(18,2) NULL,
    DT_ATUALIZACAO    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);

/* 2.5  Dimensão VENDEDOR  (Tipo 1) */
IF OBJECT_ID('dim.DIM_VENDEDOR','U') IS NOT NULL
    DROP TABLE dim.DIM_VENDEDOR;
CREATE TABLE dim.DIM_VENDEDOR (
    VENDEDOR_SK       INT           IDENTITY(1,1)  PRIMARY KEY,
    COD_VENDEDOR      INT           NOT NULL UNIQUE,
    NOME              VARCHAR(200)  NULL,
    REGIAO            VARCHAR(100)  NULL,
    DT_ADMISSAO       DATE          NULL,
    DT_ATUALIZACAO    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);

/* 2.6  Fato VENDAS  */
IF OBJECT_ID('fact.FATO_VENDAS','U') IS NOT NULL
    DROP TABLE fact.FATO_VENDAS;
CREATE TABLE fact.FATO_VENDAS (
    FATO_ID           BIGINT        IDENTITY(1,1)  PRIMARY KEY,
    DATA_SK           INT           NOT NULL,
    CLIENTE_SK        INT           NOT NULL,
    PRODUTO_SK        INT           NOT NULL,
    VENDEDOR_SK       INT           NOT NULL,
    QTDE              INT           NOT NULL,
    VALOR_UNITARIO    DECIMAL(18,2) NOT NULL,
    VL_TOTAL          AS (QTDE * VALOR_UNITARIO) PERSISTED,
    COD_VENDA_ORIGEM  BIGINT        NOT NULL,              -- natural key p/ idempotência
    CONSTRAINT UX_FATO_VENDAS_UK UNIQUE (COD_VENDA_ORIGEM) -- evita duplicação em reprocessos
);

-- Chaves estrangeiras
ALTER TABLE fact.FATO_VENDAS  WITH CHECK
    ADD CONSTRAINT FK_FATO_DIM_TEMPO     FOREIGN KEY (DATA_SK)    REFERENCES dim.DIM_TEMPO   (DATA_SK),
        CONSTRAINT FK_FATO_DIM_CLIENTE   FOREIGN KEY (CLIENTE_SK)  REFERENCES dim.DIM_CLIENTE(CLIENTE_SK),
        CONSTRAINT FK_FATO_DIM_PRODUTO   FOREIGN KEY (PRODUTO_SK)  REFERENCES dim.DIM_PRODUTO(PRODUTO_SK),
        CONSTRAINT FK_FATO_DIM_VENDEDOR  FOREIGN KEY (VENDEDOR_SK) REFERENCES dim.DIM_VENDEDOR(VENDEDOR_SK);

-- Índices de performance de leitura
CREATE INDEX IX_FATO_VENDAS_DATA ON fact.FATO_VENDAS (DATA_SK) INCLUDE (VL_TOTAL, QTDE);
CREATE INDEX IX_FATO_VENDAS_CLIENTE ON fact.FATO_VENDAS (CLIENTE_SK);
CREATE INDEX IX_FATO_VENDAS_PRODUTO ON fact.FATO_VENDAS (PRODUTO_SK);
CREATE INDEX IX_FATO_VENDAS_VENDEDOR ON fact.FATO_VENDAS (VENDEDOR_SK);

/* =========================================================
   3.   ETL / CARGA – exemplo de upsert SCD Tipo 2 (CLIENTE)
   =========================================================
   Exemplo mínimo, assumindo dados frescos em stg.STG_CLIENTE.
   Para cada execução, o MERGE garante idempotência e insere
   nova versão somente se algum atributo relevante mudou.
   ========================================================= */

-- Hash MD5 dos atributos (evita comparação campo-a-campo)
USE COMERCIO_DW;
GO
WITH src AS (
    SELECT
        c.COD_CLIENTE,
        c.NOME,
        c.CPF,
        c.EMAIL,
        c.DT_NASCIMENTO,
        CONVERT(BINARY(16), HASHBYTES('MD5',
              CONCAT(c.NOME, '|', c.CPF, '|', c.EMAIL, '|', c.DT_NASCIMENTO))) AS HASH_ATTR
    FROM COMERCIO_STAGE.stg.STG_CLIENTE c
)
MERGE dim.DIM_CLIENTE AS tgt
USING src
      ON  tgt.COD_CLIENTE = src.COD_CLIENTE
      AND tgt.ATIVO = 1
WHEN MATCHED AND tgt.HASH_ATTR <> src.HASH_ATTR THEN        -- houve mudança
    UPDATE SET
        tgt.ATIVO = 0,
        tgt.DT_FIM = CAST(GETDATE() AS DATE)        -- encerra versão antiga
WHEN NOT MATCHED BY TARGET THEN                         -- novo cliente
    INSERT (COD_CLIENTE, NOME, CPF, EMAIL, DT_NASCIMENTO,
            DT_INICIO, ATIVO, HASH_ATTR)
    VALUES (src.COD_CLIENTE, src.NOME, src.CPF, src.EMAIL, src.DT_NASCIMENTO,
            CAST(GETDATE() AS DATE), 1, src.HASH_ATTR)
OUTPUT $action, inserted.*, deleted.*;
GO

/* ======  14/09/2025 Modelagem e Estruturação da Solução de Dados - concluído ====== */

