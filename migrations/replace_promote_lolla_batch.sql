-- Run this in the Supabase SQL Editor for project xoxo (qbeoejuivhurmprizzwo)
-- Replaces promote_lolla_batch with proper field mapping:
--   - Cleans phone/whatsapp (rejects > 15 digits, strips non-digits)
--   - Converts age text ("31-35 ans") to integer
--   - Converts languages text[] to jsonb [{language, rate}]
--   - Maps services array
--   - Sets avatar_url from first photo
--   - Inserts media records with 'approved' status
--   - Generates proper slug

CREATE OR REPLACE FUNCTION bronze.promote_lolla_batch(batch_size int DEFAULT 25)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $fn$
DECLARE
  rec record;
  new_id uuid;
  v_canton_id int;
  v_city_id int;
  v_promoted int := 0;
  v_skip_city int := 0;
  v_skip_cat int := 0;
  v_batch int;
  v_cat text;
  v_avatar text;
  v_phone text;
  v_wa text;
  v_age_num int;
  v_langs jsonb;
BEGIN
  -- Randomize batch ±30%
  v_batch := greatest(1, batch_size + floor(random() * batch_size * 0.6 - batch_size * 0.3)::int);

  FOR rec IN
    SELECT *
    FROM bronze.lolla_ads
    WHERE promoted_at IS NULL
      AND phone IS NOT NULL
      AND photo_urls IS NOT NULL
      AND array_length(photo_urls, 1) > 0
    ORDER BY random()
    LIMIT v_batch
  LOOP
    -- Map canton: "Vaud (VD)" → match on "Vaud"
    v_canton_id := NULL;
    IF rec.canton IS NOT NULL THEN
      SELECT id INTO v_canton_id
      FROM public.cantons
      WHERE name ILIKE split_part(rec.canton, ' (', 1)
      LIMIT 1;
    END IF;
    IF v_canton_id IS NULL THEN
      v_skip_city := v_skip_city + 1;
      CONTINUE;
    END IF;

    -- Map city
    v_city_id := NULL;
    IF rec.city IS NOT NULL THEN
      SELECT id INTO v_city_id
      FROM public.cities
      WHERE name ILIKE rec.city AND canton_id = v_canton_id
      LIMIT 1;
      IF v_city_id IS NULL THEN
        SELECT id INTO v_city_id
        FROM public.cities
        WHERE name ILIKE rec.city
        LIMIT 1;
      END IF;
    END IF;

    -- Map category
    v_cat := CASE rec.category
      WHEN 'escort-girls' THEN 'girls'
      WHEN 'escort-trans' THEN 'trans'
      WHEN 'massage-sensuel' THEN 'massage'
      WHEN 'massage' THEN 'massage'
      WHEN 'webcams' THEN 'tv'
      WHEN 'sm-bdsm' THEN 'sm'
      WHEN 'sex-phone' THEN 'girls'
      WHEN 'location-d' THEN 'salon'
      ELSE NULL
    END;
    IF v_cat IS NULL THEN
      v_skip_cat := v_skip_cat + 1;
      CONTINUE;
    END IF;

    -- Clean phone: strip non-digits, cap at 11 for Swiss
    v_phone := regexp_replace(rec.phone, '[^0-9]', '', 'g');
    IF length(v_phone) > 11 AND v_phone LIKE '41%' THEN
      v_phone := left(v_phone, 11);
    END IF;

    -- Clean WhatsApp: reject garbage
    v_wa := rec.whatsapp;
    IF v_wa IS NOT NULL AND length(v_wa) > 15 THEN
      v_wa := v_phone;
    END IF;

    -- Avatar from first photo
    v_avatar := rec.photo_urls[1];

    -- Parse age: "31-35 ans" → 31
    v_age_num := NULL;
    IF rec.age IS NOT NULL THEN
      BEGIN
        v_age_num := substring(rec.age from '(\d+)')::int;
      EXCEPTION WHEN OTHERS THEN
        v_age_num := NULL;
      END;
    END IF;

    -- Convert languages: text[] → jsonb [{language, rate}]
    v_langs := '[]'::jsonb;
    IF rec.languages IS NOT NULL AND array_length(rec.languages, 1) > 0 THEN
      SELECT jsonb_agg(jsonb_build_object('language', lang, 'rate', 3))
      INTO v_langs
      FROM unnest(rec.languages) AS lang;
    END IF;

    -- Insert listing
    INSERT INTO public.listings_ads (
      canton_id, city_id, category, nickname, description,
      professional_phone, whatsapp_number, avatar_url,
      age, spoken_languages, services, onlyfans_link,
      status, approved, hidden, owner_id, slug
    ) VALUES (
      v_canton_id, v_city_id, v_cat,
      left(rec.nickname, 50),
      rec.description,
      v_phone,
      COALESCE(v_wa, v_phone),
      v_avatar,
      v_age_num,
      v_langs,
      COALESCE(rec.services, ARRAY[]::text[]),
      rec.onlyfans_url,
      'active', true, false,
      '8a20194c-c560-49c8-bfb7-1d3f727ceba6'::uuid,
      lower(regexp_replace(
        COALESCE(rec.nickname, 'ad') || '-' || COALESCE(rec.city, 'ch') || '-' || rec.lolla_id::text,
        '[^a-zA-Z0-9-]', '-', 'g'
      ))
    )
    RETURNING id INTO new_id;

    -- Insert all photos as approved media
    IF rec.photo_urls IS NOT NULL THEN
      INSERT INTO public.media (listing_id, type, url, status, created_at)
      SELECT
        new_id,
        CASE WHEN ordinality = 1 THEN 'avatar' ELSE 'photo' END,
        u,
        'approved',
        now()
      FROM unnest(rec.photo_urls) WITH ORDINALITY AS t(u, ordinality);
    END IF;

    -- Mark as promoted
    UPDATE bronze.lolla_ads
    SET promoted_at = now(), promoted_listing_id = new_id
    WHERE id = rec.id;

    v_promoted := v_promoted + 1;
  END LOOP;

  RETURN jsonb_build_object(
    'promoted_count', v_promoted,
    'skipped_no_city', v_skip_city,
    'skipped_no_category', v_skip_cat
  );
END;
$fn$;
