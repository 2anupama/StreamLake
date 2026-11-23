DO $$
BEGIN
    EXECUTE format('ALTER ROLE %I WITH REPLICATION', current_user);
END
$$;

CREATE TABLE companies (
    company_id TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    subsidiary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE policies (
    policy_id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL REFERENCES companies(company_id),
    policy_name TEXT NOT NULL,
    sum_insured NUMERIC(14, 2) NOT NULL CHECK (sum_insured > 0),
    record_status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE employees (
    employee_id TEXT PRIMARY KEY,
    employee_name TEXT NOT NULL,
    email TEXT NOT NULL,
    mobile_number TEXT,
    gender TEXT NOT NULL,
    date_of_birth DATE NOT NULL,
    joining_date DATE NOT NULL,
    company_id TEXT NOT NULL REFERENCES companies(company_id),
    company_name TEXT NOT NULL,
    subsidiary TEXT,
    department TEXT NOT NULL,
    designation TEXT NOT NULL,
    sum_insured NUMERIC(14, 2) NOT NULL CHECK (sum_insured > 0),
    policy_id TEXT NOT NULL REFERENCES policies(policy_id),
    relationship TEXT NOT NULL DEFAULT 'Self',
    record_status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dependents (
    dependent_id TEXT PRIMARY KEY,
    employee_id TEXT NOT NULL REFERENCES employees(employee_id) ON DELETE CASCADE,
    dependent_name TEXT NOT NULL,
    email TEXT,
    mobile_number TEXT,
    gender TEXT NOT NULL,
    date_of_birth DATE NOT NULL,
    relationship TEXT NOT NULL,
    record_status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE companies REPLICA IDENTITY FULL;
ALTER TABLE policies REPLICA IDENTITY FULL;
ALTER TABLE employees REPLICA IDENTITY FULL;
ALTER TABLE dependents REPLICA IDENTITY FULL;

CREATE PUBLICATION streamlake_publication
    FOR TABLE companies, policies, employees, dependents;