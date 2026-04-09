# MarketWatch

A modular premarket market monitoring pipeline for ingesting market data, generating sector-level and equity-level signals, and producing structured daily reports. This pipeline is part of a broader effort to build systematic macro + cross-asset monitoring tools for trading and portfolio decision-making.

---

## Overview

MarketWatch is an actively developed research and automation project designed to support systematic market monitoring and decision-making workflows.

The system ingests market data, computes cross-asset and equity-level signals, and generates structured daily artifacts for macro interpretation, idea generation, and downstream reporting.

This project is built with a focus on:

- modular design
- reproducibility
- scalability across data sources and signal pipelines

---

## Current Features

### 1. Market Data Pipeline

- Daily price ingestion via Polygon API
- Cross-asset proxy tracking across equities, rates, credit, commodities, crypto, and volatility

---

### 2. Sector & Macro Snapshot

Generates a daily sector overview including:

- S&P 500 (SPY)
- Nasdaq (QQQ)
- Gold (GLD)
- Oil (USO)
- Long Treasuries (TLT)
- Credit (LQD / HYG)
- US Dollar (UUP)
- Volatility proxy (VXX)
- Sector ETFs (XLE, XLF)
- Thematic indices (SOXX, IBIT)

Example output:

SECTOR OVERVIEW  
symbol | name | ret_d  
GLD | Gold | 0.009681  
HYG | High Yield Credit | 0.000251  
IBIT | Bitcoin | -0.010684  
LQD | Investment Grade Credit | 0.001101  
QQQ | Nasdaq | 0.000153  
SOXX | Chips | 0.010580  
SPY | S&P 500 | 0.000440  
TLT | Long Treasuries | -0.000114  
USO | Oil | -0.006209  
UUP | US Dollar | -0.002880  
VXX | VIX | 0.033094  
XLE | Energy | 0.008010  
XLF | Financials | 0.000000  

Treasury HY spread: -0.000366  

---

### 3. Equity Signal Engine

Daily screening across a dynamic universe using:

- rolling return features (daily / weekly / monthly)
- EWMA-based relative strength
- cross-sectional ranking vs benchmark (SPY)
- z-score normalization

Produces outputs such as:

- top relative strength names
- abnormal movement flags
- volatility spike detection

Example output:

VolSpikeFlags: (none)  
RecentAbnormalFlags: PANW  

Top 10 by ratio_ewma_vs_21:

- AI
- IBIT
- IONQ
- ZS
- PSTG
- RGTI
- QBTS
- CSCO
- NET
- ZETA

(Full metrics include daily/weekly/monthly returns, EWMA ratios vs 21-day baseline and SPY, and z-scores.)

---

### 4. Report Generation

- Structured text-based artifacts
- Organized by date
- Designed for downstream email / dashboard integration

---

## Project Structure

MarketWatch/  
├─ jobs/ → orchestration (premarket pipeline entry point)  
├─ src/  
│  ├─ common/ → shared helpers  
│  ├─ prices/ → market data ingestion (Polygon)  
│  ├─ options/ → options pipeline (in progress)  
│  ├─ news/ → news parsing (in progress)  
│  ├─ reddit/ → sentiment pipeline (in progress)  
│  ├─ processor/ → report assembly and formatting  
│  └─ utility/ → misc utilities (dates, paths, etc.)  
├─ .env.example  
├─ .gitignore  
├─ requirements.txt  
└─ README.md  

---

## Project Status

This is an actively developed project.

### Currently implemented

- Polygon-based price ingestion
- sector / macro overview
- equity signal engine
- report artifact generation

### In progress

- options analytics module
- news parsing pipeline
- Reddit sentiment ingestion
- enhanced reporting and distribution layer

---

## Design Philosophy

This project is structured as a pipeline system rather than a monolithic script.

Key principles:

- modularity — each data source or signal lane is isolated
- composability — new pipelines can be added without breaking existing ones
- separation of concerns — ingestion, processing, and reporting are independent
- scalability — designed to expand across data sources and workflows

---

## Notes

- Data and generated reports are intentionally excluded from the repository
- API keys and secrets are managed through environment variables
- Output shown above is a sample from the current working pipeline
