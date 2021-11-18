-- migrate:up
CREATE TABLE tag_history (
  tag_id
    BYTES NOT NULL REFERENCES tag (id),
  updated_at
    TIMESTAMPTZ(0) NOT NULL,
  updated_by
    BYTES NULL REFERENCES member (id),
  deleted
    BOOL NOT NULL,
  cross_language_id
    BYTES NOT NULL,
  name
    STRING NOT NULL,
  slug
    STRING NOT NULL,
  description
    STRING NOT NULL,
  PRIMARY KEY (tag_id, updated_at)
);
GRANT SELECT ON TABLE tag_history TO api_read;
GRANT SELECT, INSERT ON TABLE tag_history TO api_write;

-- migrate:down
DROP TABLE tag_history;