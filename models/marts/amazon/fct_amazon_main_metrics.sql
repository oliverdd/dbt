with amazon_main_metrics as (

  select 
      s.sku,
      s.asin,
      round(zeroifnull(m.price),2) as "Price",
      sp."AcOS %" as "AcOS %",
      sp."RoAS $" as "RoAS $",
      round((sp."CM Total"+m."CM Total")/(sp."Y Sales"+m."Y Sales")*100.0,2) as "CM %",
      round(zeroifnull(m."Y Units"+sp."Y Units"),0) as "Y Units",
      round(zeroifnull(m."Y3 Units"+sp."Y3 Units"),0) as "Y3 Units",
      round(zeroifnull(m."Y7 Units"+sp."Y7 Units"),0) as "Y7 Units",
      round(zeroifnull(m."Y30 Units"+sp."Y30 Units"),0) as "Y30 Units",
      round(zeroifnull(m."Y Sales"+sp."Y Sales"),0) as "Y Sales",
      round(zeroifnull(m."Y3 Sales"+sp."Y3 Sales"),0) as "Y3 Sales",
      round(zeroifnull(m."Y7 Sales"+sp."Y7 Sales"),0) as "Y7 Sales",
      round(zeroifnull(m."Y30 Sales"+sp."Y30 Sales"),0) as "Y30 Sales"
  from {{ ref('amazon_skus_asins') }} s 
  full join {{ ref('fct_amazon_mws_metrics') }} m on s.sku = m.sku
  full join {{ ref('fct_amazon_sp_metrics') }} sp on s.sku = sp.sku

)

select 
    case when sku = 'YT-OP4S-K9QG' then 'mctvanilla'
        when sku = 'jointsupplement' then 'gluco-msm'
        when sku = 'FQ-YDTC-RKGQ' then 'basecoffee'
        when sku = 'unflavoredcoffee' then 'ketocoffeeunfl'
        when sku = 'P4-2CR3-LZWB' then 'ketogreenslem'
        when sku = 'almondbars' then 'pkbar-almondbrownie'
        when sku = 'mindunflav' then 'mindunfl'
        when sku = 'B07H8QV3DC' then 'ketocoffeevanilla'
        when sku = 'ketocollagenchoc-5pack' then 'ketocollagenchoc-5pack'
        when sku = 'caramelmct' then 'mctcaramel'
        when sku = 'bloodsugarcaps' then 'bloodsugarsupport'
        when sku = 'C8C10Blend32oz' then 'mctliquidblend'
        when sku = '755899993338' then 'strips-amazon'
        when sku = 'DI-X3FR-2JLK' then 'carbsvanilla'
        when sku = 'plantchoc' then 'plant-choc'
        when sku = 'UJ-GULM-D319' then 'basevanilla-mct'
        when sku = 'FQ-YDTC-RKGQ.error4' then 'basechoc-mct'
        when sku = 'c8MCTLiquid32oz' then 'mctliquidc8'
        when sku = 'mctmatcha-5pack' then 'mctmatcha-5pack'
        when sku = 'mochacoffee' then 'ketocoffeemocha'
        when sku = 'GU-IT5D-RI5T.error3' then 'basepeach-mct'
        when sku = 'UJ-GULM-D319.error' then 'basecoffee'
        when sku = 'GU-IT5D-RI5T.error' then 'basecoffee'
        when sku = 'LS-OVA8-VB2P' then 'ketocollagenchoc'
        when sku = '5O-FNIQ-RAGX' then 'applecidervinegar'
        when sku = 'AF-TQ2M-JB9L' then 'mctmatcha'
        when sku = '755899993307' then 'journal'
        when sku = '65-51J0-GBSA' then 'basecoffee-mct'
        when sku = 'livercaps' then 'beefliver'
        when sku = 'GU-IT5D-RI5T.error0703' then 'basepeach'
        when sku = 'ketocollagenunfl-5pack' then 'ketocollagenunfl-5pack'
        when sku = 'UA-ISXK-V7HC' then 'primevanilla'
        when sku = 'minimctblend' then 'minimctblend'
        when sku = 'UJ-GULM-D319.errors' then 'basevanilla'
        when sku = 'electrolytes' then 'electrolytes'
        when sku = 'JD-VSDX-AKHF' then 'ketogreensorange'
        when sku = 'VC-8JYE-2G52' then 'greensberry'
        when sku = 'ketonootropic' then 'ketonootropics'
        when sku = '6B-LJHL-RIR4' then 'turmeric'
        when sku = '42-U2YN-4YQT' then 'mctchoc'
        when sku = 'VM-7LTH-XN8A' then 'plant-van'
        when sku = 'collagencaps' then 'mctcollagencaps'
        when sku = '5pkmctunflv' then 'mctunfl-5pack'
        when sku = '65-51J0-GBSA.error' then 'basecoffee'
        when sku = 'DC-BMXG-UEMK' then 'ketocollagenunfl'
        when sku = 'cinnamoncaps' then 'cinnamon'
        when sku = 'KO-ZAUX-5P5W' then 'purewodbb'
        when sku = 'caramelbase' then 'basecaramel-mct'
        when sku = 'vanillacoffee' then 'ketocoffeevanilla'
        when sku = 'GU-IT5D-RI5T' then 'basepeach-mct'
        when sku = '8Q-82VT-OSZV' then 'mctunfl'
        when sku = 'vanillaprotein' then 'ketocollagenvanilla'
        when sku = 'nutbuttervanilla' then 'ketobutter-maccash'
        when sku = '32-ZIXX-ENDO' then 'primechoc'
        when sku = '4P-9JYK-FJ3B' then 'collagenunfl'
        when sku = 'caramelcollagen' then 'ketocollagencaramel'
        when sku = 'JV-0YUC-K7VD' then 'perform'
        when sku = 'FQ-YDTC-RKGQ' then 'basechoc'
        when sku = 'mindchoc' then 'mindchoc'
        when sku = 'mctcap300' then 'mctsoftgel'
        when sku = '8D-ILBU-TJYE' then 'purewodgoji' else sku end as sku,
    asin,
    "Price",
    "AcOS %",
    "RoAS $",
    "CM %",
    "Y Units",
    "Y3 Units",
    "Y7 Units",
    "Y30 Units",
    "Y Sales",
    "Y3 Sales",
    "Y7 Sales",
    "Y30 Sales"  
from amazon_main_metrics
order by "Y30 Sales" desc 