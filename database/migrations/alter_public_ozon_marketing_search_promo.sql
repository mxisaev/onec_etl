-- Alter table public.ozon_marketing_search_promo
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN sourceSku TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN imageUrl TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN bidPrice TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN previousBid_bid TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN previousBid_bidPrice TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN previousBid_updatedAt TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN views_thisWeek TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN views_previousWeek TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN visibilityIndex TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN previousVisibilityIndex TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN hint_campaignId TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN hint_organisationTitle TEXT;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN searchPromoStatus BOOLEAN;
ALTER TABLE public.ozon_marketing_search_promo ADD COLUMN isSearchPromoAvailable BOOLEAN;
ALTER TABLE public.ozon_marketing_search_promo ALTER COLUMN sku TYPE TEXT USING sku::TEXT;
ALTER TABLE public.ozon_marketing_search_promo ALTER COLUMN title TYPE TEXT USING title::TEXT;
ALTER TABLE public.ozon_marketing_search_promo ALTER COLUMN price TYPE TEXT USING price::TEXT;
ALTER TABLE public.ozon_marketing_search_promo ALTER COLUMN bid TYPE TEXT USING bid::TEXT;