# 🧾 INSTRUÇÕES TÉCNICAS - Pipeline ETL

Este arquivo contém as orientações básicas para auxiliar na execução.

---

## 📁 ARQUIVOS FORNECIDOS

- COMERCIO_OLTP.BAK → Backup do banco de dados transacional
- template_python.ipynb → Notebook base
- README.txt → Este documento com instruções

---

## 🛠️ REQUISITOS DO AMBIENTE

- SQL Server 2019 (ou superior)
- Visual Studio com SQL Server Integration Services (SSIS)
- Python 3.8+ com Jupyter Notebook
- Power BI Desktop
- Bibliotecas Python:
  - pandas
  - sqlalchemy
  - pyodbc
  - matplotlib
  - seaborn

---

## 🧩 ETAPAS GERAIS 

1. **Restaurar o backup `COMERCIO_OLTP.BAK`** no SQL Server
2. Criar os bancos `COMERCIO_STAGE` e `COMERCIO_DW`
3. Realizar a modelagem dimensional e staging conforme estabelecido
4. Criar o processo de ETL completo utilizando SSIS
5. Validar os dados no DW com Python (ver passo 4)
6. Desenvolver as consultas SQL avançadas (ver passo 5)
7. Construir o dashboard no Power BI (ver passo 6)

---

## 📦 ESTRUTURA ESPERADA DO PROJETO FINAL

Compactar os arquivos em `.zip` com a seguinte estrutura:

📂 Entrega_Candidato_Nome.zip  
├── projeto_ssis\\                  → Projeto do SSIS com pacotes `.dtsx`  
├── job_sql_agent.txt              → Script SQL ou print da configuração do job  
├── modelo_python.ipynb            → Notebook Jupyter com validações  
├── queries_dw.sql                 → Consultas SQL avançadas  
├── dashboard_dw.pbix              → Painel Power BI  
└── README.txt                     → Este arquivo  
