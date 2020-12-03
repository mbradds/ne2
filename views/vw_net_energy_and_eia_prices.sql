IF OBJECT_ID ( 'vw_net_energy_and_eia_prices', 'v' ) IS NOT NULL   
    DROP view vw_net_energy_and_eia_prices;  
GO 

create view vw_net_energy_and_eia_prices

as

SELECT
net.SettlementDate, 
net.Market, 
net.Instrument, 
net.SettlementPriceDifferential, 
net.SettlementPriceImplied, 
net.[WTI CMA], 
net.InstrumentStartDate, 
net.InstrumentEndDate, 
net.[First Trade Date], 
net.[Last Trade Date], 
net.[Enbridge NOS], 
wti.[WTI Spot], 
brent.[Brent Spot]
FROM dbo.vw_net_energy_implied AS net 
INNER JOIN
(SELECT Date, Value AS [Brent Spot]
FROM dbo.EIA_Prices_Oil
WHERE (Market = 'Europe Brent') AND ([Delivery Period] = 'Spot Price FOB')) AS brent ON net.SettlementDate = brent.Date 
INNER JOIN
(SELECT  Date, 
Value AS [WTI Spot]
FROM dbo.EIA_Prices_Oil
WHERE (Market = 'Cushing, Ok') AND ([Delivery Period] = 'Spot Price FOB')) AS wti ON net.SettlementDate = wti.Date