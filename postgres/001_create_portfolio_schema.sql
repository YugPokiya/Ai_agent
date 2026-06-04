CREATE TABLE IF NOT EXISTS developer_profiles (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    title TEXT NOT NULL,
    location TEXT NOT NULL,
    summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS skills (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER NOT NULL REFERENCES developer_profiles(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    years NUMERIC(4, 1) NOT NULL CHECK (years >= 0)
);

CREATE TABLE IF NOT EXISTS projects (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER NOT NULL REFERENCES developer_profiles(id) ON DELETE CASCADE,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    summary TEXT NOT NULL,
    technologies TEXT[] NOT NULL DEFAULT '{}',
    highlights TEXT[] NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_skills_category ON skills(category);
CREATE INDEX IF NOT EXISTS idx_projects_technologies ON projects USING GIN(technologies);
