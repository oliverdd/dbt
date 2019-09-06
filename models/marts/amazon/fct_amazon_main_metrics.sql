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
    case when sku = 'LS-OVA8-VB2P' then 'ketocollagenchoc'
        when sku = '755899993338' then 'strips-amazon'
        when sku = 'FQ-YDTC-RKGQ' then 'basechoc'
        when sku = 'vanillaprotein' then 'ketocollagenvanilla'
        when sku = 'YT-OP4S-K9QG' then 'mctvanilla'
        when sku = 'caramelbars' then 'pkbar-saltedcaramel'
        when sku = 'caramelcollagen' then 'ketocollagencaramel'
        when sku = 'caramelmct' then 'mctcaramel'
        when sku = '8Q-82VT-OSZV' then 'mctunfl'
        when sku = '42-U2YN-4YQT' then 'mctchoc'
        when sku = 'pkbar-cb' then 'pkbar-cb'
        when sku = 'almondbars' then 'pkbar-almondbrownie'
        when sku = 'lemonpoppybars' then 'pkbar-lemonpoppy'
        when sku = 'UJ-GULM-D319' then 'basevanilla'
        when sku = 'caramelbase' then 'basecaramel'
        when sku = 'bhbcaps' then 'bhbcaps'
        when sku = '65-51J0-GBSA' then 'basecoffee'
        when sku = 'nutbuttervanilla' then 'ketobutter-maccash'
        when sku = 'c8MCTLiquid32oz' then 'mctliquidc8'
        when sku = 'GU-IT5D-RI5T' then 'basepeach'
        when sku = 'DC-BMXG-UEMK' then 'ketocollagenunfl'
        when sku = 'pkbar-scx3' then 'pkbar-scx3'
        when sku = 'nutbuttervanilla' then 'ketobutter-maccash'
        when sku = 'C8C10Blend32oz' then 'mctliquidblend'
        when sku = 'electrolytes' then 'electrolytes'
        when sku = 'mctcap300' then 'mctsoftgel'
        when sku = 'JV-0YUC-K7VD' then 'perform'
        when sku = 'caramelbars-fbm' then 'pkbar-saltedcaramel'
        when sku = 'stripsSL' then 'strips-amazon'
        when sku = '32-ZIXX-ENDO' then 'primechoc'
        when sku = 'AF-TQ2M-JB9L' then 'mctmatcha'
        when sku = 'ketonootropic' then 'ketonootropics'
        when sku = 'ketowheychoc' then 'ketowheychoc'
        when sku = 'UA-ISXK-V7HC' then 'primevanilla'
        when sku = 'P4-2CR3-LZWB' then 'ketogreenslem'
        when sku = '4P-9JYK-FJ3B' then 'collagenunfl'
        when sku = 'pkbar-cd' then 'pkbar-cd'
        when sku = 'ketowheyvanilla' then 'ketowheyvanilla'
        when sku = 'bloodsugarcaps' then 'bloodsugarsupport'
        when sku = 'pkstarterbundle-choc' then 'pkstarterbundle-choc'
        when sku = 'vanillacoffee' then 'ketocoffeevanilla'
        when sku = 'mochacoffee' then 'ketocoffeemocha'
        when sku = 'bhbcapsx2' then 'bhbcapsx2'
        when sku = 'barbundle-ASL' then 'barbundle-ASL'
        when sku = '8D-ILBU-TJYE' then 'purewodgoji'
        when sku = 'pkbar-abbx3' then 'pkbar-abbx3'
        when sku = 'AF-TQ2M-JB9L' then 'mctmatcha'
        when sku = 'pkbar-lpx3' then 'pkbar-lpx3'
        when sku = 'pkstarterbundle-vanilla' then 'pkstarterbundle-vanilla'
        when sku = 'lemonpoppybars-fbm' then 'pkbar-lemonpoppy'
        when sku = 'krillmct' then 'krillmct'
        when sku = 'VC-8JYE-2G52' then 'greensberry'
        when sku = 'ketocollagenchoc-5pack' then 'ketocollagenchoc-5pack'
        when sku = '6B-LJHL-RIR4' then 'turmeric'
        when sku = 'electrolytesx2' then 'electrolytesx2'
        when sku = 'ketowheyunfl' then 'ketowheyunfl'
        when sku = 'ketobutter-maccashx2' then 'ketobutter-maccashx2'
        when sku = '5pkmctunflv' then 'mctunfl-5pack'
        when sku = 'pkbar-cdx3' then 'pkbar-cdx3'
        when sku = 'unflavoredcoffee' then 'ketocoffeeunfl'
        when sku = 'basechoc-5pack' then 'basechoc-5pack'
        when sku = 'livercaps' then 'beefliver'
        when sku = 'minimctblend.fba' then 'minimctblend'
        when sku = 'VM-7LTH-XN8A' then 'plant-van'
        when sku = 'essentials-choc' then 'essentials-choc'
        when sku = 'mctmatcha-5pack' then 'mctmatcha-5pack'
        when sku = 'ketocollagenunfl-5pack' then 'ketocollagenunfl-5pack'
        when sku = 'basepeach-5pack' then 'basepeach-5pack'
        when sku = '5O-FNIQ-RAGX' then 'applecidervinegar'
        when sku = 'bhbstrips' then 'bhbstrips'
        when sku = 'basevanilla-5pack' then 'basevanilla-5pack'
        when sku = 'coffeebundle' then 'coffeebundle'
        when sku = 'cinnamoncaps' then 'cinnamon'
        when sku = 'basecoffee-5pack' then 'basecoffee-5pack'
        when sku = 'pkstarterbundle-caramel' then 'pkstarterbundle-caramel'
        when sku = 'jointsupplement' then 'gluco-msm' else sku end as sku,
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