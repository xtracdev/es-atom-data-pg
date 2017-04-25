CREATE TABLE IF NOT EXISTS t_aeae_atom_event(
    id bigserial,
    feedid CHARACTER VARYING(100),
    event_time TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CLOCK_TIMESTAMP(),
    aggregate_id CHARACTER VARYING(60) NOT NULL,
    version NUMERIC(38,0) NOT NULL,
    typecode CHARACTER VARYING(30) NOT NULL,
    payload BYTEA,
    primary key(id)
)
WITH (
    OIDS=FALSE
);

CREATE TABLE IF NOT EXISTS t_aefd_feed(
    id bigserial,
    event_time TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CLOCK_TIMESTAMP(),
    feedid CHARACTER VARYING(100) NOT NULL,
    previous CHARACTER VARYING(100),
    primary key(id)
)
WITH (
    OIDS=FALSE
);

CREATE INDEX aeaenn_feedid
ON t_aeae_atom_event
USING BTREE (feedid ASC);

CREATE INDEX aefdnn_feedid
ON t_aefd_feed
USING BTREE (feedid ASC);

CREATE INDEX aefdnn_previous
ON t_aefd_feed
USING BTREE (previous ASC);