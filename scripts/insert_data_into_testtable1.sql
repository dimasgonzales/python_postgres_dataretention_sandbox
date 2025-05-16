INSERT INTO public.test_table1 (foo, mtime)
    SELECT generate_series(1, 100), NOW();
