with source as (
    select
        amazonorderid,
        buyeremail,  
        buyername,
        convert_timezone('America/Los_Angeles', purchasedate)::date as day,
        item.value:SellerSKU::string as sku,
        item.value:ASIN::string as asin,
        item.value:ItemPrice:Amount::float as price,
        item.value:ItemTax:Amount::float as item_tax,
        item.value:PromotionDiscount:Amount::float as discount,
        item.value:PromotionDiscountTax:Amount::float as discount_tax,
        item.value:ProductInfo:NumberOfItems::int as numberofitems,
        item.value:QuantityOrdered::int as quantityordered,
        item.value:ItemPrice:Amount::float * item.value:QuantityOrdered::int as sales,
        ordertotal:Amount::float as order_total,
        orderstatus,
        isbusinessorder,
        item.value:IsGift::string as isgift
    from raw.perfect_keto_amazon_mws.orders,
    lateral flatten (input => orderitems) item
),

asins as (
    select
        distinct asin
    from source
), 

costs as (
  select
      asin,
      case when asin = 'B072KFZ9TQ' then 7.37
          when asin = 'B07QZT2RTK' then 2.81
          when asin = 'B01M7XI35O' then 12.45
          when asin = 'B01MUB7BUV' then 0.68
          when asin = 'B01MUB7BUV' then 0.68
          when asin = 'B07SPW6V2X' then 3.49
          when asin = 'B077J7FGRF' then 9.8
          when asin = 'B0797ZSZDW' then 7.37
          when asin = 'B07J1YD6MY' then 12.4
          when asin = 'B06VVJLSZR' then 9.8
          when asin = 'B07FL1PW5W' then 7.37
          when asin = 'B07JBP3151' then 12.4
          when asin = 'B077J34GD4' then 9.8
          when asin = 'B07FFFBWFL' then 9.8
          when asin = 'B079KKM9BK' then 6
          when asin = 'B076PT9MT8' then 12.45
          when asin = 'B07JCPKW1T' then 12.4
          when asin = 'B0786NDJXX' then 10.1
          when asin = 'B075L5HD6H' then 7.37
          when asin = 'B07FF9TC1M' then 12.45
          when asin = 'B076PMSD72' then 12.45
          when asin = 'B076FCD2KM' then 9.8
          when asin = 'B01M4R32QX' then 12.45
          when asin = 'B0786Q3BZ6' then 6.5
          when asin = 'B07FN5G6QC' then 4.09
          when asin = 'B0751379Q9' then 15.36
          when asin = 'B072N28S7B' then 12.89
          when asin = 'B07BYQGCXX' then 12.49
          when asin = 'B072N1T2RN' then 12.89
          when asin = 'B07KFQS6KF' then 11.49
          when asin = 'B07FN3PJTG' then 6.75
          when asin = 'B07H8QV3DC' then 3.92
          when asin = 'B07KFQVBT9' then 11.49
          when asin = 'B07H8R7DDM' then 3.92
          when asin = 'B07NPW9HSR' then 10.9
          when asin = 'B0786Q46VR' then 13.42
          when asin = 'B07H8PQ8Z5' then 3.92
          when asin = 'B07MFZTHSP' then 10.25
          when asin = 'B07KFQ2463' then 11.49
          else 0 end as cogs,
      case when asin = 'B072KFZ9TQ' then 5.8485
          when asin = 'B07QZT2RTK' then 4.4985
          when asin = 'B01M7XI35O' then 8.8485
          when asin = 'B01MUB7BUV' then 1.1985
          when asin = 'B01MUB7BUV' then 1.1985
          when asin = 'B07SPW6V2X' then 4.9485
          when asin = 'B077J7FGRF' then 5.85
          when asin = 'B0797ZSZDW' then 5.8485
          when asin = 'B07J1YD6MY' then 5.9985
          when asin = 'B06VVJLSZR' then 4.76
          when asin = 'B07FL1PW5W' then 5.8485
          when asin = 'B07JBP3151' then 5.9985
          when asin = 'B077J34GD4' then 4.76
          when asin = 'B07FFFBWFL' then 4.76
          when asin = 'B079KKM9BK' then 2.8455
          when asin = 'B076PT9MT8' then 8.8485
          when asin = 'B07JCPKW1T' then 5.9985
          when asin = 'B0786NDJXX' then 5.2485
          when asin = 'B013MRPPL6' then 7.4955
          when asin = 'B075L5HD6H' then 5.8485
          when asin = 'B07FF9TC1M' then 8.8485
          when asin = 'B076PMSD72' then 8.8485
          when asin = 'B076FCD2KM' then 5.8485
          when asin = 'B01M4R32QX' then 8.8485
          when asin = 'B0786Q3BZ6' then 4.0485
          when asin = 'B07FN5G6QC' then 4.35
          when asin = 'B01M2Y1NN8' then 7.4955
          when asin = 'B0751379Q9' then 7.4985
          when asin = 'B072N28S7B' then 7.4985
          when asin = 'B07BYQGCXX' then 8.85
          when asin = 'B071SH212S' then 4.3485
          when asin = 'B072N1T2RN' then 7.4445
          when asin = 'B071G8WTK9' then 5.9985
          when asin = 'B07KFQS6KF' then 5.9985
          when asin = 'B07FN3PJTG' then 4.35
          when asin = 'B07H8QV3DC' then 2.8455
          when asin = 'B07KFQVBT9' then 4.95
          when asin = 'B07H8R7DDM' then 2.8455
          when asin = 'B013P4YVU2' then 8.25
          when asin = 'B07NPW9HSR' then 4.0485
          when asin = 'B07G7BTTMC' then 3.3
          when asin = 'B01GIIOP4Y' then 7.4985
          when asin = 'B07FYTH417' then 3.3
          when asin = 'B0786Q46VR' then 5.9985
          when asin = 'B07KT6NWTW' then 3.3
          when asin = 'B07KT9QGXM' then 3.3
          when asin = 'B071W7HS67' then 3.861
          when asin = 'B07H8PQ8Z5' then 2.8455
          when asin = 'B07MFZTHSP' then 4.4985
          when asin = 'B07FRRW4QC' then 2.9985
          when asin = 'B07G4MW7TH' then 3.3
          when asin = 'B07KTB9T7Z' then 3.3
          when asin = 'B07MW8LSL9' then 1.1985
          when asin = 'B07KT8KJGL' then 3.3
          when asin = 'B07KFQ2463' then 5.9985
          when asin = 'B07G7CPQTM' then 3.3
          when asin = 'B074YXP8GX' then 2.8425
          when asin = 'B07FRN1R39' then 4.35
          when asin = 'B074YQSX11' then 2.8425
          when asin = 'B07FRPTRPD' then 2.9985
          when asin = 'B07FPPT9BT' then 2.9985
          else 0 end as amazon_referral,
      case when asin = 'B072KFZ9TQ' then 4.76
          when asin = 'B07QZT2RTK' then 2.42
          when asin = 'B01M7XI35O' then 3.28
          when asin = 'B01MUB7BUV' then 3.19
          when asin = 'B01MUB7BUV' then 2.08
          when asin = 'B07SPW6V2X' then 3.28
          when asin = 'B077J7FGRF' then 4.76
          when asin = 'B0797ZSZDW' then 4.76
          when asin = 'B07J1YD6MY' then 8.8
          when asin = 'B06VVJLSZR' then 4.76
          when asin = 'B07FL1PW5W' then 4.76
          when asin = 'B07JBP3151' then 13.41
          when asin = 'B077J34GD4' then 4.76
          when asin = 'B07FFFBWFL' then 4.76
          when asin = 'B079KKM9BK' then 4.76
          when asin = 'B076PT9MT8' then 3.28
          when asin = 'B07JCPKW1T' then 4.76
          when asin = 'B0786NDJXX' then 5.26
          when asin = 'B013MRPPL6' then 5.26
          when asin = 'B075L5HD6H' then 4.76
          when asin = 'B07FF9TC1M' then 3.28
          when asin = 'B076PMSD72' then 3.28
          when asin = 'B076FCD2KM' then 3.28
          when asin = 'B01M4R32QX' then 3.28
          when asin = 'B0786Q3BZ6' then 4.76
          when asin = 'B07FN5G6QC' then 3.19
          when asin = 'B01M2Y1NN8' then 5.26
          when asin = 'B0751379Q9' then 4.76
          when asin = 'B072N28S7B' then 3.28
          when asin = 'B07BYQGCXX' then 4.76
          when asin = 'B071SH212S' then 4.76
          when asin = 'B072N1T2RN' then 3.28
          when asin = 'B071G8WTK9' then 4.76
          when asin = 'B07KFQS6KF' then 4.76
          when asin = 'B07FN3PJTG' then 3.28
          when asin = 'B07H8QV3DC' then 3.19
          when asin = 'B07KFQVBT9' then 4.76
          when asin = 'B07H8R7DDM' then 3.19
          when asin = 'B013P4YVU2' then 4.76
          when asin = 'B07NPW9HSR' then 2.41
          when asin = 'B07G7BTTMC' then 3.19
          when asin = 'B01GIIOP4Y' then 4.76
          when asin = 'B07FYTH417' then 3.19
          when asin = 'B0786Q46VR' then 5.26
          when asin = 'B071W7HS67' then 3.19
          when asin = 'B07H8PQ8Z5' then 3.19
          when asin = 'B07MFZTHSP' then 3.19
          when asin = 'B07FRRW4QC' then 3.28
          when asin = 'B07G4MW7TH' then 3.19
          when asin = 'B07MW8LSL9' then 3.19
          when asin = 'B07KFQ2463' then 4.76
          when asin = 'B07G7CPQTM' then 3.19
          when asin = 'B074YXP8GX' then 4.76
          when asin = 'B07FRN1R39' then 3.19
          when asin = 'B074YQSX11' then 4.76
          when asin = 'B07FRPTRPD' then 3.19
          when asin = 'B07FPPT9BT' then 3.19
          else 0 end as amazon_fba_fee
  from asins
  
),

calculations as (

    select 
        s.*,
        c.cogs*s.quantityordered as cogs,
        c.amazon_referral*s.quantityordered as amazon_referral,
        c.amazon_fba_fee*s.quantityordered as amazon_fba_fee
    from source s
    left join costs c on s.asin = c.asin

),

final as (

    select 
        *,
        sales-cogs-amazon_referral-amazon_fba_fee as contribution_margin
    from calculations

)
select * from final
