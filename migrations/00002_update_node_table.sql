-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';
ALTER TABLE public.node ADD advertise_addr varchar NULL;
ALTER TABLE public.node ADD "lock" bigint NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
ALTER TABLE public.node DROP COLUMN "lock";
ALTER TABLE public.node DROP COLUMN advertise_addr;
-- +goose StatementEnd
