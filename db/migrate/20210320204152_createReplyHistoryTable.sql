-- migrate:up
CREATE TABLE reply_history (
  reply_topic_id
    BYTES NOT NULL,
  reply_id
    BYTES NOT NULL,
  updated
    TIMESTAMPTZ(0) NOT NULL,
  content
    STRING NOT NULL,
  PRIMARY KEY (reply_topic_id, reply_id, updated),
  FOREIGN KEY (reply_topic_id, reply_id)
    REFERENCES reply (topic_id, id)
);
GRANT SELECT ON TABLE reply_history TO api_read;
GRANT SELECT, INSERT ON TABLE reply_history TO api_write;

-- migrate:down
DROP TABLE reply_history;