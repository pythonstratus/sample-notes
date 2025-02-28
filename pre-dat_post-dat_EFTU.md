# Environment Process Flow Diagram

## Overview
This diagram illustrates the data processing workflows across different environments (DEV, TEST, PRE, PROD), highlighting the differences in EFTU (Enterprise File Transfer Utility) availability and process orchestration between environments.

## Key Components

### Environments
- **DEV → MOCK**: Development environment using mock data
- **TEST → MOCK**: Testing environment using mock data
- **PRE → LIVE**: Pre-production environment using live data
- **PROD → LIVE**: Production environment using live data

### EFTU Status
- **DEV**: No EFTU available
- **TEST**: No EFTU available
- **PRE**: EFTU available
- **PROD**: EFTU available

### Process Types
- **DEV**: Manual orchestration with no automation, using Control M
- **TEST**: Manual orchestration with no automation
- **PRE**: Live end-to-end (E2E) processing
- **PROD**: Live end-to-end (E2E) processing

## Process Flows

### PRE/PROD Flow (with EFTU)
1. EFTU retrieves and prepares input data
2. Input data is passed to the POST API
3. POST API triggers the Entity ETL Process Code

### DEV/TEST Flow (without EFTU)
1. Pre-Dat Jobs run to simulate EFTU functionality
2. Generated input data is passed to the POST API
3. POST API triggers the Entity ETL Process Code

## Special Considerations
- Pre-Dat Jobs are required in DEV and TEST environments to compensate for the absence of EFTU
- Different environments use different data sources (mock vs. live)
- Process orchestration varies by environment (manual vs. automated)

## Legend
- Pink nodes: Environment designations
- Yellow nodes: EFTU status indicators
- Blue nodes: Process type details
- Red node: Pre-Dat Jobs (specific to DEV/TEST environments)

## Usage Notes
- This diagram helps teams understand the differences in data processing workflows across environments
- When deploying changes, be aware of the different orchestration methods in each environment
- Pre-Dat Jobs in DEV/TEST environments must be maintained to ensure proper simulation of EFTU functionality
