-- 0. Создаем таблицы 
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT DEFAULT current_user,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);


-- 1. Создайте функцию логирования изменений по трем полям.
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
DECLARE
    change_record RECORD;
BEGIN
    IF NEW.name IS DISTINCT FROM OLD.name THEN
        INSERT INTO users_audit(user_id, field_changed, old_value, new_value)
        VALUES (OLD.id, 'name', OLD.name, NEW.name);
    END IF;
    
    IF NEW.email IS DISTINCT FROM OLD.email THEN
        INSERT INTO users_audit(user_id, field_changed, old_value, new_value)
        VALUES (OLD.id, 'email', OLD.email, NEW.email);
    END IF;
    
    IF NEW.role IS DISTINCT FROM OLD.role THEN
        INSERT INTO users_audit(user_id, field_changed, old_value, new_value)
        VALUES (OLD.id, 'role', OLD.role, NEW.role);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- 2. Создайте trigger на таблицу users.
CREATE OR REPLACE TRIGGER trigger_log_user_changes
AFTER UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_changes();


-- 3. Установите расширение pg_cron.
CREATE EXTENSION IF NOT EXISTS pg_cron;


-- 4. Создайте функцию
CREATE OR REPLACE FUNCTION export_daily_audit()
RETURNS void AS $$
DECLARE
    export_path TEXT;
    export_query TEXT;
BEGIN
    export_path := '/tmp/users_audit_export_' || to_char(CURRENT_DATE, 'YYYYMMDD') || '.csv';
    
    export_query := format($$
        COPY (
            SELECT *
            FROM users_audit
            WHERE changed_at >= CURRENT_DATE - INTERVAL '1 day'
              AND changed_at < CURRENT_DATE
        ) to %L with CSV HEADER
    $$, export_path);
    
    EXECUTE export_query;
    
    RAISE NOTICE 'Exported audit data to: %', export_path;
END;
$$ LANGUAGE plpgsql;


-- 5. Установите планировщик pg_cron на 3:00 ночи.
SELECT cron.schedule(
    'daily-audit-export',         
    '0 3 * * *',                  
    $$SELECT export_daily_audit()$$  
);

