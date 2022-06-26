---------------------------------------------------
-- iTESCO
---------------------------------------------------

-- najit posledni uvadeny nazev
create or replace temp table "itesco_most_recent_itemName" as
select "itemId",
       "itemName",
       "currentPrice",
       "itemUrl",
       row_number() over (partition by "itemId" order by "date"::date desc) as rn
from "itesco_clean"
where "itemId"::string <> ''
  and "itemId" is not null
  and "itemName"::string <> ''
  and "itemName" is not null
    qualify rn = 1;

-- "itesco_nazev" = 'KAKAOVÝ PRÁŠEK' - scraplo se 2x, oznacit typ searche
create table "itesco_search_bot_v2_u" as
select distinct *,
                case
                    when "itesco_nazev" = 'KAKAOVÝ PRÁŠEK' and "productId" = '2005100242357' and "poradi" = 1
                        then 'stringSearch'
                    when "itesco_nazev" = 'KAKAOVÝ PRÁŠEK' and "productId" = '2005100241989' and "poradi" = 2
                        then 'stringSearch'
                    when "itesco_nazev" = 'KAKAOVÝ PRÁŠEK' then 'urlSearch'
                    else 'stringSearch'
                    end as "searchType"
from "itesco_search_bot_v2";

-- tabulka, ze ktere se cistilo
create or replace table "search_bot_itesco_to_clean" as
select *
from (select distinct "productId" || '_' || replace(trim("csu_id"), '.', '_') as "productId_csuId_ident"
                    , t2."csu_nazev"
                    , "itesco_notes"
                    , t3."itemName"
                    , t3."itemUrl"
                    , coalesce(t1."itesco_nazev", t2."itesco_nazev")          as "itesco_nazev"
                    , t1."productId"
                    , t1."poradi"
                    , t1."searchType"
                    , t2."csu_id"
      from "itesco_search_bot_v2_u" t1
               full outer join "search_bot_testing_results" t2
                               on t1."itesco_nazev" = t2."itesco_nazev"
               left join "itesco_most_recent_itemName" t3
                         on t1."productId" = t3."itemId")
where "poradi" <= 8
order by "itesco_nazev";

-- uprava tabulky id to delete
create or replace table "itesco_id_to_delete_u" as
select distinct trim(id) as "id"
from "itesco_id_to_delete";

-- kategorie s malym poctem produktu -> manualni search
create temp table "itesco_okurky_doplneni" as
select "itemId"::string || '_E01_173_02' as "productId_csuId_ident",
       'STERILOVANÉ OKURKY'              as "csu_nazev",
       null                              as "itesco_notes",
       "itemName",
       "itemUrl",
       'NAKLÁDANÉ OKURKY'                as "itesco_nazev",
       "itemId"                          as "productId",
       null                              as "poradi",
       'manualSearch'                    as "searchType",
       'E01_173_02'                      as "csu_id"
from (select *, row_number() over (partition by "itemId" order by "date" desc) as rown
      from "itesco_clean"
      where "itemId" in
            (2001020257049,
             2001020280504,
             2001120365900,
             2001020279094,
             2001020279095,
             2001014802596,
             2001005130356,
             2001120367028,
             2001019076381,
             2001120351675)
          qualify rown = 1);

create temp table "itesco_kompot_doplneni" as
select "itemId"::string || '_E01_164_01'     as "productId_csuId_ident",
       'KOMPOT MERUŇKOVÝ (EVENT. BROSKVOVÝ)' as "csu_nazev",
       null                                  as "itesco_notes",
       "itemName",
       "itemUrl",
       'BROSKVE V NÁLEVU'                    as "itesco_nazev",
       "itemId"                              as "productId",
       null                                  as "poradi",
       'manualSearch'                        as "searchType",
       'E01_164_01'                          as "csu_id"
from (select *, row_number() over (partition by "itemId" order by "date" desc) as rown
      from "itesco_clean"
      where "itemId" in
            (2001019074868,
             2001020229952,
             2001018528133,
             2001019074844,
             2001017545049)
          qualify rown = 1);

create temp table "itesco_margarin_doplneni" as
select "itemId"::string || '_E01_152_01' as "productId_csuId_ident",
       'ROSTLINNÝ ROZTÍRATELNÝ TUK'      as "csu_nazev",
       null                              as "itesco_notes",
       "itemName",
       "itemUrl",
       'MARGARÍN'                        as "itesco_nazev",
       "itemId"                          as "productId",
       null                              as "poradi",
       'manualSearch'                    as "searchType",
       'E01_152_01'                      as "csu_id"
from (select *, row_number() over (partition by "itemId" order by "date" desc) as rown
      from "itesco_clean"
      where "itemId" in
            (
             2001020091629,
             2001020097891,
             2001005224567,
             2001019454202,
             2001005224628,
             2001014461625,
             2001009727125,
             2001018037413)
          qualify rown = 1);

-- vymazat nesmyslne produkty z itesco_id_to_delete & pridat manualne nalezene produkty
create or replace table "search_bot_itesco_clean_vero" as
(select "productId_csuId_ident",
        "csu_nazev",
        "itesco_notes",
        "itemName",
        "itemUrl",
        "itesco_nazev",
        "productId"::int as "productId", -- jinak by to hazelo float
        "poradi",
        "searchType",
        "csu_id"
 from "search_bot_itesco_to_clean" t1
          left join "itesco_id_to_delete_u" t2
                    on t1."productId_csuId_ident" = t2."id"
 where t2."id" is null)
union all
select "productId_csuId_ident",
       "csu_nazev",
       "itesco_notes",
       "itemName",
       "itemUrl",
       "itesco_nazev",
       "productId"::int as "productId",
       "poradi",
       "searchType",
       "csu_id"
from "itesco_okurky_doplneni"
union all
select "productId_csuId_ident",
       "csu_nazev",
       "itesco_notes",
       "itemName",
       "itemUrl",
       "itesco_nazev",
       "productId"::int as "productId",
       "poradi",
       "searchType",
       "csu_id"
from "itesco_kompot_doplneni"
union all
select "productId_csuId_ident",
       "csu_nazev",
       "itesco_notes",
       "itemName",
       "itemUrl",
       "itesco_nazev",
       "productId"::int as "productId",
       "poradi",
       "searchType",
       "csu_id"
from "itesco_margarin_doplneni";

-- duplicita
delete
from "search_bot_itesco_clean_vero"
where "productId_csuId_ident" = '2005100242357_E01_213_01'
  and "searchType" = 'stringSearch';

-- problem se zmenou id stejneho produktu - naparovani pomoci EAN
create or replace temp table "itesco_kos_ean_itemId_array" as
select ean,
       array_agg("itemId")                 as "itemId_array",
       case
           when "itemId_array"[0]::int < "itemId_array"[1]::int then "itemId_array"[0]::int
           else "itemId_array"[1]::int end as "itemId_1", -- mensi id prvni
       case
           when "itemId_array"[0]::int > "itemId_array"[1]::int then "itemId_array"[0]::int
           else "itemId_array"[1]::int end as "itemId_2"  -- vetsi id druhy
from (select split_part("itemImage", '/', 7) ean, "itemId", max(1) as "grouper"
      from "itesco_clean"
      where split_part("itemImage", '/', 7) in
            (select distinct split_part("itemImage", '/', 7) as ean
             from "itesco_clean"
             where "itemId" in
                   (select distinct "productId"
                    from "search_bot_itesco_clean_vero")
               and "itemImage" is not null
               and left("itemImage", 5) = 'https'
               and right("itemImage", 12) <> 'no-image.gif')
      group by 1, 2)
group by 1;

-- EAN ma max dve id, proto pridat alternativni id k tabulce ze search bota ocistene od nesmyslnych produktu
create or replace table "search_bot_itesco_clean_vero_ean_fix" as
select t1.*,
       case
           when t1."productId" = t2."itemId_1" then "itemId_2"
           when t1."productId" = t2."itemId_2" then "itemId_1"
           end as "productId_alt"
from "search_bot_itesco_clean_vero" t1
         left join (select distinct "itemId_1", "itemId_2"
                    from "itesco_kos_ean_itemId_array"
                    where "itemId_1" < "itemId_2") t2
                   on t1."productId" = t2."itemId_1"
                       or t1."productId" = t2."itemId_2"
order by "productId_alt";

-- napojeni na historii z hlidace
create or replace table "itesco_spotrebni_kos_vyvoj_ean_fix" as
select t1."productId_csuId_ident",
       t1."csu_nazev",
       case
           when t1."productId_alt" is not null then array_construct(t1."productId", t1."productId_alt")
           else array_construct(t1."productId") end                      as "found_product_ids",
       t1."itemName",
       t1."nove_poradi",
       t2."p_key",
       t2."itemId"                                                       as "itemId_given_day",
       t2."itemUrl"                                                      as "itemUrl_given_day",
       t2."currentPrice",
       t2."originalPrice",
       COALESCE(NULLIF("originalPrice", ''), NULLIF("currentPrice", '')) as "basePrice",
       t2."date",
       t2."itemImage"                                                    as "itemImage_given_day"
from (select *,
             row_number() over (partition by "csu_nazev" order by "poradi") as "nove_poradi"
      from "search_bot_itesco_clean_vero_ean_fix") t1
         left join "itesco_clean" t2
                   on t1."productId" = t2."itemId"
                       or t1."productId_alt" = t2."itemId"
order by t2."date";

-- Tesco kosik, obohateny o data zo scrapingu + preratana price per unit
--- tato tabulka by mala riesit problem s vazenymi potravinami (ked je nieco v zlave, tak miesto ceny za hmotnost balenia Tesco uvadza cenu za kilo)
CREATE OR REPLACE TABLE "itesco_tmp1" AS
WITH "pom" AS (SELECT *,
                      CASE
                          WHEN "itemId_given_day" = '2001120571270' AND TO_NUMBER("basePrice") > '70'
                              THEN NULLIF("basePrice", '') -- Tesco tu mota dve rozne gramaze, predpokladam, ze ta vyssia cena je za 1kg
                          ELSE DIV0(NULLIF("basePrice", ''), NULLIF("package_amount", ''))::DECIMAL(38, 2)
                          END                                                                   AS "price_per_unit",
                      CASE
                          WHEN "unit_price_per_unit" <> '' THEN "unit_price_per_unit"
                          WHEN "unit_price_per_unit" = '' AND "package_unit" <> '' THEN "package_unit"
                          ELSE NULL
                          END                                                                   AS "unit",
                      "price_per_unit" / NULLIF("price_new", '') * NULLIF("package_amount", '') AS "flag"
               FROM (SELECT *
                     FROM "itesco_spotrebni_kos_vyvoj_ean_fix"
                     WHERE ARRAY_SIZE("found_product_ids") = 1) t1
                        LEFT JOIN "itesco_extra_info_clean" t2
                                  ON t2."itemId"::bigint = t1."found_product_ids"[0]::bigint

               UNION ALL

               SELECT *,
                      CASE
                          WHEN "itemId_given_day" = '2001120571270' AND TO_NUMBER("basePrice") > '70'
                              THEN NULLIF("basePrice", '') -- Tesco tu mota dve rozne gramaze, predpokladam, ze ta vyssia cena je za 1kg
                          ELSE DIV0(NULLIF("basePrice", ''), NULLIF("package_amount", ''))::DECIMAL(38, 2)
                          END                                                                   AS "price_per_unit",
                      CASE
                          WHEN "unit_price_per_unit" <> '' THEN "unit_price_per_unit"
                          WHEN "unit_price_per_unit" = '' AND "package_unit" <> '' THEN "package_unit"
                          ELSE NULL
                          END                                                                   AS "unit",
                      "price_per_unit" / NULLIF("price_new", '') * NULLIF("package_amount", '') AS "flag"
               FROM (SELECT *
                     FROM "itesco_spotrebni_kos_vyvoj_ean_fix"
                     WHERE ARRAY_SIZE("found_product_ids") > 1) t1
                        LEFT JOIN "itesco_extra_info_clean" t2
                                  ON t2."itemId"::bigint = t1."found_product_ids"[0]::bigint OR
                                     t2."itemId"::bigint = t1."found_product_ids"[1]::bigint)
SELECT *,
       (CASE
            WHEN "flag" > 3 AND "originalPrice" <> '' THEN "originalPrice"
            ELSE "price_per_unit"
           END)::DECIMAL(38, 2) AS "ppu_final"
FROM "pom"
;

/* tu sa zasa riesia jednodnove prepady, ked zabudli prehodit zlacnenu cenu, ale kedze je Tesco NEUVERITELNE KOKOTSKE, nakoniec kazdy den po skonceni zlavy beriem predchadzajucu cenu */
CREATE OR REPLACE TABLE "itesco_spotrebni_kos_vyvoj_ppu_pom" AS
SELECT "productId_csuId_ident",
       "csu_nazev",
       "found_product_ids",
       "itemName",
       "nove_poradi",
       "p_key",
       "itemId_given_day",
       "itemUrl_given_day",
       "currentPrice",
       "originalPrice",
       "basePrice",
       "date",
       "itemImage_given_day",
       "title",
       "category1",
       "category2",
       "category3",
       "contact",
       "country",
       "country_new",
       "ingredients",
       "itemId",
       "price",
       "price_new",
       "currency",
       "pricePerUnit",
       "kc_price_per_unit",
       "unit_price_per_unit",
       "unitNumber",
       "unitUnit",
       "package_amount",
       "package_unit",
       CASE
           WHEN LEAD("originalPrice") OVER (PARTITION BY "itemId_given_day" ORDER BY "date") = '' AND
                LAG("originalPrice") OVER (PARTITION BY "itemId_given_day" ORDER BY "date") <> ''
               THEN LAG("ppu_final") OVER (PARTITION BY "itemId_given_day" ORDER BY "date")
           ELSE "ppu_final"
           END AS "price_per_unit",
       "unit"
FROM "itesco_tmp1"
;

-- jsou tam nejaky divno produkty,co vytvareji duplicity (title do not use atp) -> odstranit
CREATE OR REPLACE TABLE "itesco_spotrebni_kos_vyvoj_ppu_pom2" AS
SELECT *
     , CASE WHEN "title" IN ('DO NOT USE - OLD ITEM', 'Do not use') THEN 99 ELSE 1 END                                   AS "old_item_fix"
     , ROW_NUMBER() OVER (PARTITION BY "found_product_ids"[0]::bigint, "date" ORDER BY "old_item_fix", "price_per_unit") AS "old_item_fix_rn"
FROM "itesco_spotrebni_kos_vyvoj_ppu_pom"
    QUALIFY "old_item_fix_rn" = 1;

create or replace table "itesco_spotrebni_kos_vyvoj_ppu" as
select INITCAP(t2.nazev_hlavni_kategorie, '') as "csu_main_category",
       INITCAP(t2.nazev_nadkategorie, '')     as "csu_subcategory",
       CASE
           WHEN t1."csu_nazev" = 'KARLOVARSKÁ BECHEROVKA' THEN INITCAP(t1."csu_nazev")
           WHEN t1."csu_nazev" = 'FERNET STOCK' THEN INITCAP(t1."csu_nazev")
           WHEN t1."csu_nazev" = 'PRAVÝ ITALSKÝ VERMUT' THEN 'Pravý italský Vermut'
           ELSE INITCAP(t1."csu_nazev", '')
           END                                as "csu_product",
       t2.ecoicop                             as "csu_product_id",
       t2.merna_jednotka                      as "csu_amount",
       t2."empty"                             as "csu_unit",
       t2."Vaha"                              as "csu_weight",
       t1."found_product_ids"::string         as "it_product_id_array",
       t1."itemName"                          as "itemName",
       t1."nove_poradi",
       t1."p_key"                             as "p_key",
       t1."itemId_given_day",
       t1."itemUrl_given_day",
       t1."currentPrice",
       t1."originalPrice",
       t1."date",
       t1."itemImage_given_day",
       t1."basePrice",
       CASE
           WHEN "csu_product" = 'Česnek suchý' AND "package_unit" = 'ks'
               THEN 20 * "price_per_unit" * "csu_amount" -- jeden cesnek vazi cca 0.05kg = tomu odpovida price per unit, tzn krat 20 je to na kilo
           WHEN "csu_product" = 'Kiwi' AND "package_unit" = 'ks'
               THEN 12.5 * "price_per_unit" * "csu_amount" -- jedno kiwi vazi cca 0.08kg = tomu odpovida price per unit, tzn krat 12.5 je to na kilo
           WHEN "csu_product" = 'Okurky salátové' AND "package_unit" = 'ks'
               THEN 3.6 * "price_per_unit" * "csu_amount" -- jedna okura vazi cca 0.28kg = tomu odpovida price per unit, tzn krat 3.6 je to na kilo
           WHEN "csu_unit" in ('kg', 'l') THEN "price_per_unit"::DECIMAL(38, 2) * "csu_amount"
           WHEN "csu_unit" = 'g' THEN "price_per_unit" / 1000 * "csu_amount"
           WHEN "csu_unit" = 'bal.' THEN "basePrice" * "csu_amount"
           WHEN "csu_unit" = 'ks' THEN "basePrice" * "csu_amount"
           ELSE "price_per_unit"::DECIMAL(38, 2) * "csu_amount"
           END                                AS "csuRelevantPrice",
       t1."package_amount",
       t1."package_unit",
       t1."price_per_unit",
       t1."unit",
       NULLIF(t1."country_new", '')           AS "country"
from "itesco_spotrebni_kos_vyvoj_ppu_pom2" t1
         left join "csu_source_data" t2
                   on right(t1."productId_csuId_ident", 10) = trim(replace(t2.ecoicop, '.', '_'));

-- units mezi itescem a csu se obcas rozjizdej -> budeme muset brat jako interchangable kg a l, v pripade kusu overit, ze v basePrice je opravdu jen jeden kus

-- jeden ČESNEK má 0,05kg

-- dame pravidlo pro vyber produktu, ze kdyz se to nerovna unit od csu tak se nevybere
/*
"csu_product"
'Kiwi' -- musi mit velikost nebo jsem v pici - jen jeden produkt
'Okurky salátové' -- to stejny co kiwi
*/

-- nadhodnocovat ty co maji vyplnenou zemi?
-- zprumerovat 2 nejlevnejsi?
-- cim vetsi baleni, tim vetsi cena

CREATE OR REPLACE TABLE "itesco_spotrebni_kos_product_selection" AS
SELECT *
     , LAG("avg_csuRelevantPrice")
           OVER (PARTITION BY "csu_product" ORDER BY "avg_csuRelevantPrice") AS "avg_price_diff_from_prev"
FROM (SELECT *
           , STDDEV("avg_csuRelevantPrice") OVER (PARTITION BY "csu_product")               AS "stddev_avg_csuRelevantPrice"
           , AVG("avg_csuRelevantPrice") OVER (PARTITION BY "csu_product")                  AS "avg_avg_csuRelevantPrice"
           , "stddev_avg_csuRelevantPrice" / "avg_avg_csuRelevantPrice"                     as "mean_coef_var"
           , "avg_csuRelevantPrice" - "avg_avg_csuRelevantPrice"                            AS "diff_from_mean"
           , ROW_NUMBER() OVER (PARTITION BY "csu_product" ORDER BY "avg_csuRelevantPrice") AS "order_cheapest"
      FROM (SELECT "csu_product"
                 , COUNT(DISTINCT "itemName", "it_product_id_array")
                         OVER (PARTITION BY "csu_product")                                     AS "items_in_category"
                 , "itemName"
                 , "it_product_id_array"
                 , "country"
                 , "nove_poradi"
                 , "unit"
                 , "package_amount"
                 , "package_unit"
                 , CASE
                       WHEN "csu_product" = 'Droždí' AND "itemName" ILIKE '%sušené%' THEN 1
                       WHEN "csu_product" = 'Droždí' AND "itemName" ILIKE '%7g%' THEN 1
                       ELSE 0
              END                                                                              AS "unwanted"
                 , CASE
                       WHEN "csu_unit" = 'l' AND "unit" = 'kg' THEN 'kg'
                       WHEN "csu_unit" = 'kg' AND "unit" = 'l' THEN 'l'
                       ELSE "csu_unit" END                                                     AS "csu_unit_u"
                 , CASE WHEN "csu_amount" = 'g' THEN "csu_amount" / 1000 ELSE "csu_amount" END AS "csu_amount_u"
                 , COUNT(DISTINCT "date")                                                      AS "days"
                 , ROUND(AVG("csuRelevantPrice"), 2)                                           AS "avg_csuRelevantPrice"
                 , MAX("csuRelevantPrice"::DECIMAL(38, 2))                                     AS "max_csuRelevantPrice"
                 , MIN("csuRelevantPrice"::DECIMAL(38, 2))                                     AS "min_csuRelevantPrice"
            FROM "itesco_spotrebni_kos_vyvoj_ppu"
            WHERE "unwanted" = 0
            GROUP BY 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
            HAVING "days" >= 365
            ORDER BY 1, 2, "avg_csuRelevantPrice"))
WHERE "avg_csuRelevantPrice" IS NOT NULL -- CHYBI CENA => NECHCEM
ORDER BY "mean_coef_var" DESC NULLS LAST, abs("avg_price_diff_from_prev");

CREATE OR REPLACE TABLE "itesco_spotrebni_kos_product_selection_fin" AS
SELECT "csu_product"
     , "items_in_category"
     , "itemName"
     , "it_product_id_array"
     , "days" AS "days_in_history"
     , "avg_csuRelevantPrice"
     , "min_csuRelevantPrice"
     , "max_csuRelevantPrice"
     , "mean_coef_var"
     , "order_cheapest"
FROM "itesco_spotrebni_kos_product_selection";

----------------------------------------------
-- DOPLNENI HISTORIE

CREATE TEMP TABLE KOSTRA AS
SELECT DISTINCT "date"
FROM "itesco_spotrebni_kos_vyvoj_ppu" T1
ORDER BY 1;

CREATE OR REPLACE TEMP TABLE KOSTRA_2 AS
SELECT T1."date"                AS "kostra_date"
     , T2."itemName"            AS "kostra_itemName"
     , T2."it_product_id_array" AS "kostra_it_product_id_array"
FROM KOSTRA T1
         LEFT JOIN (SELECT DISTINCT "itemName", "it_product_id_array" FROM "itesco_spotrebni_kos_vyvoj_ppu") T2;

CREATE OR REPLACE TEMP TABLE KOSTRA_3 AS
SELECT *
FROM KOSTRA_2 T1
         LEFT JOIN "itesco_spotrebni_kos_vyvoj_ppu" T2
                   ON T1."kostra_date" = T2."date"
                       AND T1."kostra_itemName" = T2."itemName"
                       AND T1."kostra_it_product_id_array" = T2."it_product_id_array";

CREATE OR REPLACE TEMP TABLE "first_known_price" AS
SELECT T1.*
     , "csu_main_category"
     , "csu_subcategory"
     , "csu_product"
     , "csu_product_id"
     , "csu_amount"
     , "csu_unit"
     , "csu_weight"
     , "nove_poradi"
     , "itemId_given_day"
     , "itemUrl_given_day"
     , "itemImage_given_day"
     , "package_amount"
     , "package_unit"
     , "price_per_unit"
     , "unit"
     , "country"
     , T2."basePrice"
     , T2."currentPrice"
     , T2."originalPrice"
     , T2."csuRelevantPrice"
     , '2019-07-25' AS "first_date_of_data"
FROM (SELECT "itemName"
           , "it_product_id_array"
           , MIN("date") AS "first_date"
      FROM "itesco_spotrebni_kos_vyvoj_ppu"
      GROUP BY 1, 2) T1
         LEFT JOIN (SELECT * FROM "itesco_spotrebni_kos_vyvoj_ppu") T2
                   ON T1."itemName" = T2."itemName"
                       AND T1."first_date" = T2."date"
                       AND T1."it_product_id_array" = T2."it_product_id_array";

CREATE OR REPLACE TEMP TABLE KOSTRA_4 AS
SELECT T1."kostra_date"
     , T1."kostra_itemName"
     , T1."kostra_it_product_id_array"
     , COALESCE(T1."csu_main_category", T2."csu_main_category")     AS "csu_main_category"
     , COALESCE(T1."csu_subcategory", T2."csu_subcategory")         AS "csu_subcategory"
     , COALESCE(T1."csu_product", T2."csu_product")                 AS "csu_product"
     , COALESCE(T1."csu_product_id", T2."csu_product_id")           AS "csu_product_id"
     , COALESCE(T1."csu_amount", T2."csu_amount")                   AS "csu_amount"
     , COALESCE(T1."csu_unit", T2."csu_unit")                       AS "csu_unit"
     , COALESCE(T1."csu_weight", T2."csu_weight")                   AS "csu_weight"
     , COALESCE(T1."it_product_id_array", T2."it_product_id_array") AS "it_product_id_array"
     , COALESCE(T1."itemName", T2."itemName")                       AS "itemName"
     , COALESCE(T1."nove_poradi", T2."nove_poradi")                 AS "nove_poradi"
     , COALESCE(T1."itemId_given_day", T2."itemId_given_day")       AS "itemId_given_day"
     , COALESCE(T1."itemUrl_given_day", T2."itemUrl_given_day")     AS "itemUrl_given_day"
     , T1."currentPrice"
     , T1."originalPrice"
     , T1."date"
     , COALESCE(T1."itemImage_given_day", T2."itemImage_given_day") AS "itemImage_given_day"
     , T1."basePrice"
     , T1."csuRelevantPrice"
     , COALESCE(T1."package_amount", T2."package_amount")           AS "package_amount"
     , COALESCE(T1."package_unit", T2."package_unit")               AS "package_unit"
     , COALESCE(T1."price_per_unit", T2."price_per_unit")           AS "price_per_unit"
     , COALESCE(T1."unit", T2."unit")                               AS "unit"
     , COALESCE(T1."country", T2."country")                         AS "country"
     , T2."basePrice"                                               AS "first_basePrice"
     , T2."currentPrice"                                            AS "first_currentPrice"
     , T2."originalPrice"                                           AS "first_originalPrice"
     , T2."csuRelevantPrice"                                        AS "first_csuRelevantPrice"
     , T2."first_date_of_data"
FROM "KOSTRA_3" T1
         LEFT JOIN "first_known_price" T2
                   ON T1."kostra_itemName" = T2."itemName"
                       AND T1."kostra_date" = '2019-07-25'
                       AND T1."kostra_it_product_id_array" = T2."it_product_id_array"
ORDER BY "kostra_date";

CREATE OR REPLACE TEMP TABLE "dopocitani_non_null" AS
SELECT "kostra_itemName"                                      AS "kostra_itemName_nn"
     , "kostra_it_product_id_array"                           AS "kostra_it_product_id_array_nn"
     , "csu_main_category"                                    AS "csu_main_category_nn"
     , "csu_subcategory"                                      AS "csu_subcategory_nn"
     , "csu_product"                                          AS "csu_product_nn"
     , "csu_product_id"                                       AS "csu_product_id_nn"
     , "csu_amount"                                           AS "csu_amount_nn"
     , "csu_unit"                                             AS "csu_unit_nn"
     , "csu_weight"                                           AS "csu_weight_nn"
     , "nove_poradi"                                          AS "nove_poradi_nn"
     , "itemId_given_day"                                     AS "itemId_given_day_nn"
     , "itemUrl_given_day"                                    AS "itemUrl_given_day_nn"
     , "currentPrice"                                         AS "currentPrice_nn"
     , "originalPrice"                                        AS "originalPrice_nn"
     , "itemImage_given_day"                                  AS "itemImage_given_day_nn"
     , "package_amount"                                       AS "package_amount_nn"
     , "package_unit"                                         AS "package_unit_nn"
     , "price_per_unit"                                       AS "price_per_unit_nn"
     , "unit"                                                 AS "unit_nn"
     , "country"                                              AS "country_nn"
     , "kostra_date"                                          AS "kostra_date_nn"
     , COALESCE("csuRelevantPrice", "first_csuRelevantPrice") AS "csuRelevantPrice_nn"
     , COALESCE("basePrice", "first_basePrice")               AS "basePrice_nn"
FROM "KOSTRA_4"
WHERE "date" IS NOT NULL
   OR "first_date_of_data" IS NOT NULL;

CREATE OR REPLACE TEMP TABLE "dopocitani_non_null2" AS
SELECT *
     , LEAD("kostra_date_nn", 1, CURRENT_DATE())
            OVER (PARTITION BY "kostra_itemName_nn", "kostra_it_product_id_array_nn" ORDER BY "kostra_date_nn") AS "non_null_end"
FROM "dopocitani_non_null"
ORDER BY "kostra_date_nn";

--- pocitani historie - chybi tam pak 2019-07-25 -> nahradim 2019-07-24, aby se tam vesel i ten den
UPDATE "dopocitani_non_null2"
SET "kostra_date_nn" = '2019-07-24'
WHERE "kostra_date_nn" = '2019-07-25';

CREATE OR REPLACE TEMP TABLE "dopocitani_non_null_fin" AS
SELECT "kostra_date"                                             AS "date"
     , "kostra_itemName"                                         AS "itemName"
     , "kostra_it_product_id_array"                              AS "it_product_id_array"
     , "csu_main_category_nn"                                    AS "csu_main_category"
     , "csu_subcategory_nn"                                      AS "csu_subcategory"
     , "csu_product_nn"                                          AS "csu_product"
     , "csu_product_id_nn"                                       AS "csu_product_id"
     , "csu_amount_nn"                                           AS "csu_amount"
     , "csu_unit_nn"                                             AS "csu_unit"
     , "csu_weight_nn"                                           AS "csu_weight"
     , "nove_poradi_nn"                                          AS "nove_poradi"
     , "itemId_given_day_nn"                                     AS "itemId_given_day"
     , "itemUrl_given_day_nn"                                    AS "itemUrl_given_day"
     , "currentPrice_nn"                                         AS "currentPrice"
     , "originalPrice_nn"                                        AS "originalPrice"
     , "itemImage_given_day_nn"                                  AS "itemImage_given_day"
     , "package_amount_nn"                                       AS "package_amount"
     , "package_unit_nn"                                         AS "package_unit"
     , "price_per_unit_nn"                                       AS "price_per_unit"
     , "unit_nn"                                                 AS "unit"
     , "country_nn"                                              AS "country"
     , COALESCE(T1."csuRelevantPrice", T2."csuRelevantPrice_nn") AS "filled_csuRelevantPrice"
     , COALESCE(T1."basePrice", T2."basePrice_nn")               AS "filled_basePrice"
     , "non_null_end"                                            AS "filled_end_date"
     , 'based_on_previous'                                       AS "filled"
FROM "KOSTRA_4" T1
         LEFT JOIN "dopocitani_non_null2" T2
                   ON T1."kostra_itemName" = T2."kostra_itemName_nn"
                       AND T1."kostra_it_product_id_array" = T2."kostra_it_product_id_array_nn"
                       AND T1."kostra_date" > T2."kostra_date_nn"
                       AND T1."kostra_date" < T2."non_null_end"
WHERE "date" IS NULL
UNION ALL
SELECT "kostra_date"                AS "date"
     , "kostra_itemName"            AS "itemName"
     , "kostra_it_product_id_array" AS "it_product_id_array"
     , "csu_main_category"
     , "csu_subcategory"
     , "csu_product"
     , "csu_product_id"
     , "csu_amount"
     , "csu_unit"
     , "csu_weight"
     , "nove_poradi"
     , "itemId_given_day"
     , "itemUrl_given_day"
     , "currentPrice"
     , "originalPrice"
     , "itemImage_given_day"
     , "package_amount"
     , "package_unit"
     , "price_per_unit"
     , "unit"
     , "country"
     , "csuRelevantPrice"           AS "filled_csuRelevantPrice"
     , "basePrice"                  AS "filled_basePrice"
     , NULL                            "filled_end_date"
     , 'original'                   AS "filled"
FROM "KOSTRA_4"
WHERE "date" IS NOT NULL;

CREATE OR REPLACE TABLE "out_itesco_spotrebni_kos_full_hist" AS
SELECT T1.*
     , T2."items_in_category"
     , T2."days_in_history"
     , T2."avg_csuRelevantPrice"
     , T2."min_csuRelevantPrice"
     , T2."max_csuRelevantPrice"
     , T2."mean_coef_var"
     , T2."order_cheapest"
     , CASE WHEN T2."csu_product" IS NULL THEN 0 ELSE 1 END AS "to_include"
FROM "dopocitani_non_null_fin" T1
         LEFT JOIN "itesco_spotrebni_kos_product_selection_fin" T2
                   ON T1."csu_product" = T2."csu_product"
                       AND T1."itemName" = T2."itemName"
                       AND T1."it_product_id_array" = T2."it_product_id_array"
WHERE "date" < CURRENT_DATE();

CREATE OR REPLACE TABLE "itesco_spotrebni_kos_vazena_suma" AS
WITH csu_kos_cheapest AS
         (SELECT *
               , ROW_NUMBER() OVER (PARTITION BY "csu_product", "date" ORDER BY "filled_csuRelevantPrice") AS "order_cheapest"
          FROM "out_itesco_spotrebni_kos_full_hist"
          WHERE "filled_csuRelevantPrice" IS NOT NULL
            AND "filled_csuRelevantPrice"::STRING <> ''
            AND "csu_product" IS NOT NULL
            AND "csu_product"::STRING <> ''
            AND "date" IS NOT NULL
            AND "date"::STRING <> ''
            AND "date" < CURRENT_DATE()
              QUALIFY "order_cheapest" = 1)
SELECT *
     , ("vazena_suma" - "vazena_suma_last_date") / "vazena_suma_last_date" AS "perc_diff_last_day"
FROM (SELECT "date"
           , ROUND(SUM("filled_csuRelevantPrice" * "csu_weight"), 4) AS "vazena_suma"
      FROM csu_kos_cheapest
      GROUP BY 1) T1
         LEFT JOIN (SELECT "date"                                                  as "last_date"
                         , ROUND(SUM("filled_csuRelevantPrice" * "csu_weight"), 4) AS "vazena_suma_last_date"
                    FROM csu_kos_cheapest
                    GROUP BY 1
                    ORDER BY "date" DESC
                    LIMIT 1) T2;

-----------------------------
-- agregacni tabulky pro dash

CREATE OR REPLACE TABLE "out_itesco_spotrebni_kos_main_cat_agg" AS
SELECT "date"
     , "csu_main_category"
     , AVG("filled_csuRelevantPrice") AS "csuRelevantPrice"
FROM "out_itesco_spotrebni_kos_full_hist"
WHERE "to_include" = 1
  AND "filled_csuRelevantPrice" IS NOT NULL
  AND "filled_csuRelevantPrice"::STRING <> ''
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE OR REPLACE TABLE "out_itesco_spotrebni_kos_sub_cat_agg" AS
SELECT "date"
     , "csu_main_category"
     , "csu_subcategory"
     , AVG("filled_csuRelevantPrice") AS "csuRelevantPrice"
FROM "out_itesco_spotrebni_kos_full_hist"
WHERE "to_include" = 1
  AND "filled_csuRelevantPrice" IS NOT NULL
  AND "filled_csuRelevantPrice"::STRING <> ''
GROUP BY 1, 2, 3
ORDER BY 1, 2;

CREATE OR REPLACE TABLE "out_itesco_spotrebni_kos_product_agg" AS
SELECT "date"
     , "csu_main_category"
     , "csu_subcategory"
     , "csu_product"
     , AVG("filled_csuRelevantPrice") AS "csuRelevantPrice"
FROM "out_itesco_spotrebni_kos_full_hist"
WHERE "to_include" = 1
  AND "filled_csuRelevantPrice" IS NOT NULL
  AND "filled_csuRelevantPrice"::STRING <> ''
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;

CREATE OR REPLACE TABLE "out_itesco_spotrebni_kos_item_agg" AS
SELECT "date"
     , "csu_main_category"
     , "csu_subcategory"
     , "csu_product"
     , "itemName"
     , AVG("filled_csuRelevantPrice") AS "csuRelevantPrice"
FROM "out_itesco_spotrebni_kos_full_hist"
WHERE "to_include" = 1
  AND "filled_csuRelevantPrice" IS NOT NULL
  AND "filled_csuRelevantPrice"::STRING <> ''
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2;