CREATE TABLE public.metric_hour (
    ts timestamp NOT NULL,
    num_call int,
    total_call_duration int,
    num_call_working_hour int,
    imei_most json,
    locations json,
    updated_at timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (ts)
);

CREATE TABLE public.metric_daily (
    ts timestamp NOT NULL,
    num_call int,
    total_call_duration int,
    num_call_working_hour int,
    imei_most json,
    locations json,
    updated_at timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (ts)
);