-- migrate:up
CREATE TABLE taxon_slug (
  slug
    STRING PRIMARY KEY,
  created
    TIMESTAMPTZ(0) DEFAULT current_timestamp(0) NOT NULL,
  taxon_id
    BYTES NOT NULL REFERENCES taxon (id)
);
GRANT SELECT ON TABLE taxon_slug TO api_read;
GRANT SELECT, INSERT ON TABLE taxon_slug TO api_write;

-- migrate:down
DROP TABLE taxon_slug;
