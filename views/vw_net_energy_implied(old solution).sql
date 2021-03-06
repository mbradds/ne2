IF OBJECT_ID ( 'vw_net_energy_implied', 'v' ) IS NOT NULL   
    DROP view vw_net_energy_implied;  
GO 

create view vw_net_energy_implied

as


with diff
as
(


SELECT
[SettlementDate],
[Market],
[Instrument],
round([SettlementValue],3) as [SettlementValue],
[InstrumentStartDate],
[InstrumentEndDate],
[First Trade Date],
[Last Trade Date],
[Enbridge NOS]

FROM [EnergyData].[dbo].[Net_Energy_Spot]
where Market not in ('CS','CL')
),

cma as
(

SELECT 
[SettlementDate],
[Market],
[Instrument], 
[SettlementValue],
[InstrumentStartDate],
[InstrumentEndDate]
  
  
FROM [EnergyData].[dbo].[Net_Energy_Settlements]

where Market in ('CS') and (SettlementDate >= InstrumentStartDate and SettlementDate <= InstrumentEndDate)
and CHARINDEX('/',[Instrument]) = 0

)


select 
diff.SettlementDate,
diff.Market,
diff.Instrument,
diff.SettlementValue as [SettlementPriceDifferential],
round(cma.SettlementValue + diff.SettlementValue,3) as [SettlementPriceImplied],
cma.SettlementValue as [WTI CMA],
diff.InstrumentStartDate,
diff.InstrumentEndDate,
diff.[First Trade Date],
diff.[Last Trade Date],
diff.[Enbridge NOS] 


from diff

inner join cma on diff.SettlementDate = cma.SettlementDate

--order by diff.SettlementDate desc