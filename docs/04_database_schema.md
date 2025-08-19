graph TD
    subgraph "Configuration"
        A1[Airflow Variables] --> B1[DAX Queries]
        A1 --> C1[Dataset Configs]
        A1 --> D1[Column Mappings]
    end

    subgraph "Base ETL DAG"
        E1[Powerbi2PostgresETL] --> F1[Process Datasets]
        F1 --> G1[Execute ETL]
        G1 --> H1[Check Results]
    end

    subgraph "Specialized ETL DAG"
        I1[CompanyProductsETL] --> J1[Process Datasets]
        J1 --> K1[Execute ETL]
        K1 --> L1[Check Results]
    end

    subgraph "ETL Tasks"
        M1[Extract] --> N1[Transform]
        N1 --> O1[Load]
    end

    subgraph "Services"
        P1[PowerBI Client] --> M1
        Q1[PostgreSQL Client] --> O1
    end

    %% Connections
    B1 -.-> |DAX Query| F1
    C1 -.-> |Dataset Config| F1
    D1 -.-> |Column Mapping| F1
    B1 -.-> |DAX Query| J1
    C1 -.-> |Dataset Config| J1
    D1 -.-> |Column Mapping| J1
    F1 --> M1
    G1 --> O1
    J1 --> M1
    K1 --> O1

    %% Style definitions
    classDef config fill:#e2f0fb,stroke:#b8daff,color:#004085;
    classDef base fill:#d4edda,stroke:#c3e6cb,color:#155724;
    classDef spec fill:#fff3cd,stroke:#ffeeba,color:#856404;
    classDef task fill:#f8d7da,stroke:#f5c6cb,color:#721c24;
    classDef service fill:#d1ecf1,stroke:#bee5eb,color:#0c5460;
    
    %% Apply styles
    class A1,B1,C1,D1 config;
    class E1,F1,G1,H1 base;
    class I1,J1,K1,L1 spec;
    class M1,N1,O1 task;
    class P1,Q1 service;
