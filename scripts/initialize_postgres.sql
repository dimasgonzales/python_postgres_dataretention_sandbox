CREATE TABLE public.test_table1 (
    foo VARCHAR(5),
    mtime TIMESTAMPTZ NOT NULL
) PARTITION BY RANGE (mtime);

SELECT public.create_parent(
    p_parent_table := 'public.test_table1',
    p_control := 'mtime',
    p_interval := '1 second'
);
