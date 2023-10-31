-- API Keys
CREATE TABLE api_keys (
    api_key text PRIMARY KEY,
    auth_entity jsonb NOT NULL,
    assigned_user text NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
