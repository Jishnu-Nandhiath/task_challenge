-- Initialize database with UTC timezone settings
-- This script runs when the PostgreSQL container starts for the first time

-- Set the default timezone to UTC for the entire database
ALTER SYSTEM SET timezone = 'UTC';

-- Reload configuration to apply the timezone setting
SELECT pg_reload_conf();

-- Set timezone for the current session (for immediate effect)
SET timezone = 'UTC';
