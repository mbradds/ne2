--get the actual CMA from Nymex daily WTI settlement prices
use EnergyData;
go
declare @cmaMonth INT;
declare @cmaYear INT;
declare @cmaDay INT;

--change these dates
set @cmaMonth = 10;
set @cmaYear = 2020;
set @cmaDay = 30;

declare @fullDate VARCHAR(50);
set @fullDate = cast(@cmaYear as varchar)+'-'+cast(@cmaMonth as varchar)+'-'+cast(@cmaDay as varchar);

with cma1Month as 
(
	SELECT 
	[month],
	[open],
	[high],
	[low],
	[last],
	[change],
	[settle],
	[volume],
	[openInterest],
	[tradeDate],
	[contractType],
	[updateTime],
	[reportType],
	[product]
	FROM [EnergyData].[dbo].[Nymex_Settlements]
	where product = 'Crude Oil Future' and reportType = 'Final' and contractType = 'Spot'
	and year(tradeDate) = @cmaYear and month(tradeDate) = @cmaMonth and day(tradeDate) <= @cmaDay
) 

--select * from cma1Month
select avg(settle) as [Actual CMA] from cma1Month

--gets the CMA from net energy
SELECT 
[SettlementDate],
[Market],
[Instrument],
[SettlementValue] as [NE2 CMA]
--[InstrumentStartDate],
--[InstrumentEndDate]
FROM [EnergyData].[dbo].[Net_Energy_Settlements]
where Market = 'CS' and InstrumentStartDate <= SettlementDate and len(Instrument) <7  and SettlementDate = @fullDate
--order by SettlementDate desc

