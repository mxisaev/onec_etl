CREATE TABLE public.ozon_marketing_search_promo (
    sku TEXT,
    sourcesku TEXT,
    imageurl TEXT,
    title TEXT,
    price TEXT,
    bid TEXT,
    bidprice TEXT,
    previousbid_bid TEXT,
    previousbid_bidprice TEXT,
    previousbid_updatedat TEXT,
    views_thisweek TEXT,
    views_previousweek TEXT,
    visibilityindex TEXT,
    previousvisibilityindex TEXT,
    hint_campaignid TEXT,
    hint_organisationtitle TEXT,
    searchpromostatus BOOLEAN,
    issearchpromoavailable BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

GRANT ALL PRIVILEGES ON TABLE public.ozon_marketing_search_promo TO postgresadmin;