IF OBJECT_ID ( 'vw_net_energy_implied', 'v' ) IS NOT NULL   
    DROP view vw_net_energy_implied;  
GO 

create view vw_net_energy_implied

as

WITH 
diff AS 
(
	SELECT 
	SettlementDate,
	Market, 
	Instrument, 
	ROUND(SettlementValue, 3) AS SettlementValue, 
	InstrumentStartDate, 
	InstrumentEndDate, 
	[First Trade Date], 
	[Last Trade Date], 
	[Enbridge NOS]
	FROM dbo.Net_Energy_Spot
	WHERE (Market NOT IN ('CS', 'CL'))
), 

cma AS
(
	SELECT 
	SettlementDate, 
	Market, 
	Instrument, 
	SettlementValue, 
	InstrumentStartDate, 
	InstrumentEndDate
	FROM dbo.Net_Energy_Settlements
	WHERE (Market = 'CS')
)

SELECT 
diff.SettlementDate, 
diff.Market, 
diff.Instrument, 
diff.SettlementValue AS SettlementPriceDifferential, 
ROUND(cma.SettlementValue + diff.SettlementValue, 3) AS SettlementPriceImplied, 
cma.SettlementValue AS [WTI CMA], 
diff.InstrumentStartDate, 
diff.InstrumentEndDate, 
diff.[First Trade Date], 
diff.[Last Trade Date], 
diff.[Enbridge NOS]
FROM diff INNER JOIN cma ON diff.SettlementDate = cma.SettlementDate AND diff.Instrument = cma.Instrument