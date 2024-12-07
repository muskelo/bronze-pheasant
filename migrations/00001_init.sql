-- +goose Up
-- +goose StatementBegin
CREATE TABLE public.file (
	id bigserial NOT NULL,
	"uuid" uuid NOT NULL,
	"size" int8 DEFAULT 0 NOT NULL,
	created_at int8 DEFAULT 0 NOT NULL,
	deleted_at int8 DEFAULT 0 NOT NULL,
	state int8 DEFAULT 0 NOT NULL,
	CONSTRAINT file_pk PRIMARY KEY (id),
	CONSTRAINT file_uuid_unique UNIQUE (uuid)
);
CREATE TABLE public.node (
	id bigserial NOT NULL,
	"name" varchar NOT NULL,
	advertise_addr varchar DEFAULT '' NOT NULL,
	"lock" int8 DEFAULT 0 NOT NULL,
	CONSTRAINT node_name_unique UNIQUE (name),
	CONSTRAINT node_pk PRIMARY KEY (id)
);
CREATE TABLE public.node_file (
	node_id int8 NOT NULL,
	file_id int8 NOT NULL,
	CONSTRAINT node_file_pk PRIMARY KEY (node_id, file_id),
	CONSTRAINT node_file_file_fk FOREIGN KEY (file_id) REFERENCES public.file(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT node_file_node_fk FOREIGN KEY (node_id) REFERENCES public.node(id) ON DELETE CASCADE ON UPDATE CASCADE
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE public.node_file;
DROP TABLE public.file;
DROP TABLE public.node;
-- +goose StatementEnd
