-- Reclassify mode for edge fn classify-transactions-property-type:
-- same columns as silver_ch.next_unclassified_transactions, for a list of ids.
CREATE OR REPLACE FUNCTION silver_ch.transactions_by_ids(p_ids text[])
 RETURNS TABLE(transaction_id text, commune text, canton_code text, transaction_type text, price numeric, surface_m2 numeric, ppe_indicator boolean, ppe_pourmille_raw text, transaction_text text, buildings_raw jsonb, is_ldtr boolean, source_table text, address text)
 LANGUAGE sql
 STABLE
AS $function$
  SELECT c.transaction_id, c.commune, c.canton_code, c.transaction_type, c.price, c.surface_m2,
         c.ppe_indicator, c.ppe_pourmille_raw, c.transaction_text, c.buildings_raw,
         c.is_ldtr, c.source_table, c.address
  FROM gold_ch.core_transactions c
  WHERE c.transaction_id = ANY(p_ids)
  ORDER BY c.transaction_id;
$function$;
REVOKE ALL ON FUNCTION silver_ch.transactions_by_ids(text[]) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION silver_ch.transactions_by_ids(text[]) TO service_role;
