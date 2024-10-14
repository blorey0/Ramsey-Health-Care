/*Part 1: Data Exploration and Preparation*/
/*1. Identify and describe 2 data quality issues present in the dataset.*/
SELECT TOP (1000) [insurer_id]
      ,[episode_id]
      ,[DateOfBirth]
      ,[Postcode]
      ,[Sex]
      ,[AdmissionDate]
      ,[SeparationDate]
      ,[AR_DRG]
      ,[ModeOfSeparation]
      ,[CareType]
      ,[SourceOfReferral]
      ,[DischargeIntention]
      ,[AdmissionProviderID]
      ,[AdmissionTime]
      ,[UrgencyOfAdmission]
      ,[SeparationTime]
      ,[PrincipalDiagnosis]
      ,[Diagnosis2]
      ,[Diagnosis3]
      ,[Diagnosis4]
      ,[Principal_ProcedureCode]
      ,[ProcedureCode2]
      ,[ProcedureCode3]
      ,[AccommodationCharge]
      ,[CCU_Charges]
      ,[ICU_Charge]
      ,[TheatreCharge]
      ,[PharmacyCharge]
      ,[ProsthesisCharge]
      ,[OtherCharges]
      ,[BundledCharges]
      ,[UnplannedTheatreVisit]
      ,[InfantWeight]
      ,[Readmission28Days]
      ,[HoursMechVentilation]
      ,[PalliativeCareStatus]
      ,[Age]
  FROM [Data Insights_Synthetic Datasets].[dbo].[DataInsightsSyntheticDataset]

--In the raw file, the data type of AccommodationCharge, CCU_Charges,ICU_Charge, TheatreCharge, PharmacyCharge, ProsthesisCharge, OtherCharges and BundledCharges are formated as general. 
--When ingesting it, I standardized the format with and their type can be seen when running the code below. All the charges have been converted to decimal(18,10) except PharmacyCharges
  EXEC sp_help 'dbo.DataInsightsSyntheticDataset';

  --Data quality analysis
  SELECT
    MIN(AccommodationCharge) AS MinAccommodationCharge, MAX(AccommodationCharge) AS MaxAccommodationCharge,
    MIN(CCU_Charges) AS MinCCUCharge, MAX(CCU_Charges) AS MaxCCUCharge,
    MIN(ICU_Charge) AS MinICUCharge, MAX(ICU_Charge) AS MaxICUCharge,
    MIN(TheatreCharge) AS MinTheatreCharge, MAX(TheatreCharge) AS MaxTheatreCharge,
    MIN(PharmacyCharge) AS MinPharmacyCharge, MAX(PharmacyCharge) AS MaxPharmacyCharge,
	MIN(OtherCharges) AS MinOtherCharges, MAX(OtherCharges) AS MaxOtherCharges,
	MIN(BundledCharges) AS MinBundledCharges, MAX(BundledCharges) AS MaxBundledCharges,
	MIN(InfantWeight) AS MinInfantWeight, MAX(InfantWeight) AS MaxInfantWeight,
	MIN(HoursMechVentilation) AS MinHoursMechVentilation, MAX(HoursMechVentilation) AS MaxHoursMechVentilation,
	MIN(Age) AS MinAge, MAX(Age) AS MaxAge,
	MIN(DateOfBirth) AS MinDateOfBirth, MAX(DateOfBirth) AS MaxDateOfBirth,
	MIN(AdmissionDate) AS MinAdmissionDate, MAX(AdmissionDate) AS MaxAdmissionDate,
	MIN(SeparationDate) AS MinSeparationDate, MAX(SeparationDate) AS MaxSeparationDate,
	MIN(AdmissionTime) AS MinAdmissionTime, MAX(AdmissionTime) AS MaxAdmissionTime,
	MIN(SeparationTime) AS MinSeparationTime, MAX(SeparationTime) AS MaxSeparationTime
FROM [dbo].[DataInsightsSyntheticDataset];

--Investigate Pharmacy Charge
SELECT
    COUNT(*) AS TotalRows,
    COUNT(CASE 
            WHEN PharmacyCharge LIKE '%e%' THEN 1 
            WHEN PharmacyCharge = 'ERROR' THEN 1 
            WHEN ISNUMERIC(PharmacyCharge) = 0 THEN 1 
          END) AS TotalAnomalousRows,
    COUNT(CASE WHEN PharmacyCharge LIKE '%e%' THEN 1 END) AS ScientificNotationCount,
    COUNT(CASE WHEN PharmacyCharge = 'ERROR' THEN 1 END) AS ErrorCount,
    COUNT(CASE WHEN ISNUMERIC(PharmacyCharge) = 0 THEN 1 END) AS NonNumericCount,
    COUNT(CASE WHEN ISNUMERIC(PharmacyCharge) = 1 THEN 1 END) AS NumericCount
FROM [dbo].[DataInsightsSyntheticDataset];

--Data quality of valid numeric entries for Pharmacy Charge
SELECT
    COUNT(*) AS TotalNumericRows,
    MIN(TRY_CAST(PharmacyCharge AS DECIMAL(38, 10))) AS MinPharmacyCharge,
    MAX(TRY_CAST(PharmacyCharge AS DECIMAL(38, 10))) AS MaxPharmacyCharge,
    AVG(TRY_CAST(PharmacyCharge AS DECIMAL(38, 10))) AS AvgPharmacyCharge,
    STDEV(TRY_CAST(PharmacyCharge AS DECIMAL(38, 10))) AS StdDevPharmacyCharge
FROM [dbo].[DataInsightsSyntheticDataset]
WHERE ISNUMERIC(PharmacyCharge) = 1;

--Investigate BundledCharges and ThreatreCharges for possibility of outliers
WITH BundledChargeStats AS (
    SELECT
        AVG(BundledCharges) AS AvgBundledCharges,
        STDEV(BundledCharges) AS StdevBundledCharges
    FROM [dbo].[DataInsightsSyntheticDataset]
),
TheatreChargeStats AS (
    SELECT
        AVG(TheatreCharge) AS AvgTheatreCharge,
        STDEV(TheatreCharge) AS StdevTheatreCharge
    FROM [dbo].[DataInsightsSyntheticDataset]
)
SELECT
    episode_id,
    BundledCharges,
    TheatreCharge,
    -- Calculate Z-scores for BundledCharges
    CASE 
        WHEN ABS((BundledCharges - b.AvgBundledCharges) / b.StdevBundledCharges) > 3 THEN 'Outlier'
        ELSE 'Normal'
    END AS BundledChargeOutlierStatus,
    -- Calculate Z-scores for TheatreCharge
    CASE 
        WHEN ABS((TheatreCharge - t.AvgTheatreCharge) / t.StdevTheatreCharge) > 3 THEN 'Outlier'
        ELSE 'Normal'
    END AS TheatreChargeOutlierStatus
FROM [dbo].[DataInsightsSyntheticDataset]
CROSS JOIN BundledChargeStats b
CROSS JOIN TheatreChargeStats t
ORDER BY episode_id;


--Check for Duplicates
--identify duplicate episode_ids
WITH Duplicateepisode_id AS (
	SELECT
		episode_id,
		AR_DRG,
		PrincipalDiagnosis,
		COUNT(*) AS DuplicateCount
	FROM [dbo].[DataInsightsSyntheticDataset]
	GROUP BY episode_id, AR_DRG, PrincipalDiagnosis
	HAVING COUNT(*) > 1
),
--Count no. of unique duplicate episode_id
UniqueDuplicate_id AS (
	SELECT
		COUNT (*) AS NumberofUniqueDuplicateepisode_id
	FROM Duplicateepisode_id
	)

--Retrieve info on duplicate episode_id

SELECT
	a.episode_id,
	a.DateOfBirth,
	a.AdmissionDate,
	a.SeparationDate,
	a.AR_DRG,
	a.CareType,
	a.AdmissionProviderID,
	a.AdmissionTime,
	a.UrgencyofAdmission,
	a.SeparationTime,
	d.DuplicateCount,
	u.NumberofUniqueDuplicateepisode_id
FROM [dbo].[DataInsightsSyntheticDataset] a
INNER JOIN Duplicateepisode_id d ON a.episode_id = d.episode_id
CROSS JOIN UniqueDuplicate_id u
ORDER BY a.episode_id, a.AdmissionDate;

--Date validity
SELECT
        COUNT(*) AS TotalRows,
        COUNT(CASE WHEN SeparationDate >= AdmissionDate THEN 1 END) AS ValidDateEntry,
        COUNT(CASE WHEN SeparationDate < AdmissionDate THEN 1 END) AS InvalidDateEntry
    FROM [dbo].[DataInsightsSyntheticDataset]
;

--Validity of Age column
WITH AgeValidation AS (
    SELECT
        a. insurer_id, 
		a. episode_id,
		a. DateOfBirth,
		a. AdmissionDate,
		a. SeparationDate,
		a. Age,
		AdmissionProviderID,
		-- Calculate the actual age using DateOfBirth and AdmissionDate
		CASE
			WHEN AdmissionDate >= DateOfBirth THEN
				DATEDIFF(YEAR, DateOfBirth, AdmissionDate) +
				CASE
					WHEN MONTH(AdmissionDate) = MONTH(DateOfBirth) AND DAY(AdmissionDate) < DAY(DateOfBirth) THEN -1
					WHEN MONTH(AdmissionDate) < MONTH(DateOfBirth) THEN -1
					ELSE 0
				END
			ELSE NULL
		END AS CalculatedAge,
		-- Determine if the calculated age matches the reported age and if the date values are valid
		CASE
			WHEN AdmissionDate < DateOfBirth THEN 'Invalid Date'
			WHEN 
				DATEDIFF(YEAR, DateOfBirth, AdmissionDate) +
				CASE 
					WHEN MONTH(AdmissionDate) = MONTH(DateOfBirth) AND DAY(AdmissionDate) < DAY(DateOfBirth) THEN -1
					WHEN MONTH(AdmissionDate) < MONTH(DateOfBirth) THEN -1
					ELSE 0 
				END = Age THEN 'Valid Age'
			ELSE 'Invalid Age'
		END AS AgeStatus
	FROM [dbo].[DataInsightsSyntheticDataset] a
),
--total count of records with admissionDate and admissionprovider ID
AgeStatusSummary AS (
	SELECT
		AdmissionProviderID,
		SUM(CASE WHEN AgeStatus = 'Valid Age' THEN 1 ELSE 0 END) AS ValidAgeCount,
        SUM(CASE WHEN AgeStatus = 'Invalid Age' THEN 1 ELSE 0 END) AS InvalidAgeCount,
        SUM(CASE WHEN AgeStatus = 'Invalid Date' THEN 1 ELSE 0 END) AS InvalidDateCount
    FROM AgeValidation
    GROUP BY AdmissionProviderID, AdmissionDate
)
SELECT
    AdmissionProviderID,
    InvalidDateCount,
    InvalidAgeCount,
    ValidAgeCount
FROM AgeStatusSummary
ORDER BY  InvalidAgeCount DESC,InvalidDateCount DESC, AdmissionProviderID;

/* 2. Using the data provided create a feature that could be valuable for analysis or modelling. */
--Calculate ChargePerDay. Will exclude PharmacyCharge due to invalid data--
WITH StayCharges AS (
--Calculate Length Of Stay
	SELECT 
		a.insurer_id, 
		a.episode_id, 
		a.AdmissionDate,
		a.DateOfBirth,
		a.SeparationDate, 
		a.AdmissionProviderID,
		a.AccommodationCharge,
		a.CCU_Charges,
		a.ICU_Charge,
		a.TheatreCharge,
		a.OtherCharges,
		a.BundledCharges,
		a.AdmissionTime,
		a.SeparationTime,
		a.CareType,
		a.ModeOfSeparation,
		a.Sex,
		a.PrincipalDiagnosis,
		a.UrgencyOfAdmission,
		a.AR_DRG,
		--Create a binary feature indicating whether the AdmissionTime is during office hours (e.g., 9AM to 5PM) or outside of office hours. Include weekends as non-office hours
		CASE
			WHEN DATENAME(WEEKDAY, a.AdmissionDate) IN ('Saturday', 'Sunday') THEN 0
			WHEN CAST(AdmissionTime AS TIME) BETWEEN '09:00:00' AND '17:00:00' THEN 1
			ELSE 0
		END AS OfficeHours,
		--LengthOfstay
		CASE
			WHEN AdmissionDate IS NULL OR SeparationDate IS NULL THEN NULL
			WHEN SeparationDate < AdmissionDate THEN NULL
			ELSE DATEDIFF(DAY, AdmissionDate, SeparationDate) 
		END AS LengthOfStay,
		--AvgTotalCharges excluding prosthesischarge
		ISNULL(AccommodationCharge, 0) + ISNULL(CCU_Charges, 0) + ISNULL(ICU_Charge, 0) + ISNULL(TheatreCharge, 0) +
		ISNULL(OtherCharges, 0) + ISNULL(BundledCharges, 0) AS DailyTotalCharges,
		--Totalcharges includes prosthesischarge
		ISNULL(AccommodationCharge, 0) + ISNULL(CCU_Charges, 0) + ISNULL(ICU_Charge, 0) + ISNULL(TheatreCharge, 0) +
		ISNULL(OtherCharges, 0) + ISNULL(BundledCharges, 0) + ISNULL (ProsthesisCharge, 0) AS TotalCharges
	FROM [dbo].[DataInsightsSyntheticDataset] a
),

--Charges per Day
ChargesperDay AS (
	SELECT
		insurer_id,
		episode_id,
		AdmissionProviderID,
		AdmissionDate,
		SeparationDate,
		LengthOfStay,
		TotalCharges,
		DailyTotalCharges,
		OfficeHours,
		CareType,
		ModeOfSeparation,
		Sex,
		PrincipalDiagnosis,
		DateOfBirth,
		UrgencyOfAdmission,
		AR_DRG,
		
		CASE
			WHEN LengthOfStay = 0 THEN DailyTotalCharges
			WHEN LengthOfStay > 0 THEN DailyTotalCharges / LengthOfStay
			ELSE NULL
		END AS ChargesperDay
	FROM StayCharges
)
SELECT
    insurer_id,
    episode_id,
    AdmissionProviderID,
    AdmissionDate,
    SeparationDate,
    LengthOfStay,
    OfficeHours,
    CareType,
    ModeOfSeparation,
    Sex,
    DateOfBirth,
    PrincipalDiagnosis,
    UrgencyOfAdmission,
    AR_DRG,
    CAST(TotalCharges AS DECIMAL(18, 2)) AS TotalCharges,
    CAST(DailyTotalCharges AS DECIMAL(18, 2)) AS DailyTotalCharges,
    CAST(ChargesperDay AS DECIMAL(18, 2)) AS ChargesperDay
FROM ChargesperDay
ORDER BY ChargesperDay DESC;

--Create a table to store the export data
DROP TABLE IF EXISTS ChargesperDay_Permanenttable;
CREATE TABLE ChargesperDay_Permanenttable (
    insurer_id nvarchar(100),
    episode_id INT,
    AdmissionProviderID INT,
    AdmissionDate DATE,
    SeparationDate DATE,
    LengthOfStay INT,
    OfficeHours BIT,
    CareType NVARCHAR(50),
    ModeOfSeparation NVARCHAR(50),
    Sex NVARCHAR(10),
    DateOfBirth DATE,
    PrincipalDiagnosis NVARCHAR(100),
    UrgencyOfAdmission NVARCHAR(50),
    AR_DRG NVARCHAR(50),
    TotalCharges DECIMAL(18, 2),
    DailyTotalCharges DECIMAL(18, 2),
    ChargesperDay DECIMAL(18, 2)
);

-- Insert the data into the new table
INSERT INTO ChargesperDay_Permanenttable
SELECT 
    a.insurer_id, 
    a.episode_id, 
    a.AdmissionProviderID,
    a.AdmissionDate,
    a.SeparationDate,
    CASE 
        WHEN a.AdmissionDate IS NULL OR a.SeparationDate IS NULL THEN NULL
        WHEN a.SeparationDate < a.AdmissionDate THEN NULL
        ELSE DATEDIFF(DAY, a.AdmissionDate, a.SeparationDate) 
    END AS LengthOfStay,
    CASE 
        WHEN DATENAME(WEEKDAY, a.AdmissionDate) IN ('Saturday', 'Sunday') THEN 0
		WHEN CAST(a.AdmissionTime AS TIME) BETWEEN '09:00:00' AND '17:00:00' THEN 1 
        ELSE 0 
    END AS OfficeHours,
    a.CareType,
    a.ModeOfSeparation,
    a.Sex,
    a.DateOfBirth,
    a.PrincipalDiagnosis,
    a.UrgencyOfAdmission,
    a.AR_DRG,
    CAST(ISNULL(a.AccommodationCharge, 0) + ISNULL(a.CCU_Charges, 0) + ISNULL(a.ICU_Charge, 0) +
         ISNULL(a.TheatreCharge, 0) + ISNULL(a.OtherCharges, 0) + ISNULL(a.BundledCharges, 0) +
         ISNULL(a.ProsthesisCharge, 0) AS DECIMAL(18, 2)) AS TotalCharges,
    CAST(ISNULL(a.AccommodationCharge, 0) + ISNULL(a.CCU_Charges, 0) + ISNULL(a.ICU_Charge, 0) +
         ISNULL(a.TheatreCharge, 0) + ISNULL(a.OtherCharges, 0) + ISNULL(a.BundledCharges, 0) 
         AS DECIMAL(18, 2)) AS DailyTotalCharges,
    CASE
        WHEN DATEDIFF(DAY, a.AdmissionDate, a.SeparationDate) = 0 
        THEN CAST(ISNULL(a.AccommodationCharge, 0) + ISNULL(a.CCU_Charges, 0) + ISNULL(a.ICU_Charge, 0) +
                  ISNULL(a.TheatreCharge, 0) + ISNULL(a.OtherCharges, 0) + ISNULL(a.BundledCharges, 0)
                  AS DECIMAL(18, 2))
        ELSE CAST((ISNULL(a.AccommodationCharge, 0) + ISNULL(a.CCU_Charges, 0) + ISNULL(a.ICU_Charge, 0) +
                   ISNULL(a.TheatreCharge, 0) + ISNULL(a.OtherCharges, 0) + ISNULL(a.BundledCharges, 0)) / 
                   DATEDIFF(DAY, a.AdmissionDate, a.SeparationDate) AS DECIMAL(18, 2))
    END AS ChargesperDay
FROM [dbo].[DataInsightsSyntheticDataset] a
ORDER BY AdmissionDate ASC;

SELECT 
	* 
	FROM ChargesperDay_Permanenttable 
	ORDER BY AdmissionDate ASC;

/*Part 2: Data Analysis and Visualisation */
--Write an SQL query to calculate the total and average admissions for each month over the last two years. Include the month and year in the results.
With DateTransformed AS (
	SELECT
		insurer_id,
        episode_id,
        AdmissionProviderID,
        AdmissionDate,
        SeparationDate,
        DATEFROMPARTS(YEAR(AdmissionDate), MONTH(AdmissionDate), 1) AS AdmissionDate_new,
        DATEFROMPARTS(YEAR(SeparationDate), MONTH(SeparationDate), 1) AS SeparationDate_new,
        LengthOfStay,
        OfficeHours,
        CareType,
        ModeOfSeparation,
        Sex,
        DateOfBirth,
        PrincipalDiagnosis,
        UrgencyOfAdmission,
        AR_DRG,
        TotalCharges,
        DailyTotalCharges,
        ChargesperDay
    FROM ChargesperDay_Permanenttable
)

SELECT
	AdmissionDate_new,
	COUNT(DISTINCT episode_id) AS TotalAdmissions,
	AVG(COUNT(DISTINCT episode_id)) OVER (PARTITION BY YEAR(AdmissionDate_new)) AS AvgAdmissionsPerYear
FROM
	DateTransformed
GROUP BY
	AdmissionDate_new
ORDER BY
	AdmissionDate_new ASC;

	--Distribution of TotalCharges by Sex
WITH TotalChargesbySex AS (
	SELECT
		TotalCharges,
		Sex,
		CUME_DIST() OVER(PARTITION BY Sex ORDER BY TotalCharges) AS CumulativePercentilebySex
	FROM
	ChargesperDay_Permanenttable
)

SELECT
    Sex,
    MAX(CASE WHEN CumulativePercentilebySex <= 0.25 THEN TotalCharges END) AS "25th Percentile",
    MAX(CASE WHEN CumulativePercentilebySex <= 0.50 THEN TotalCharges END) AS "50th Percentile (Median)",
    MAX(CASE WHEN CumulativePercentilebySex <= 0.75 THEN TotalCharges END) AS "75th Percentile",
    MAX(TotalCharges) AS "Max TotalCharges",
    MIN(TotalCharges) AS "Min TotalCharges",
	CAST(AVG(TotalCharges) AS Decimal(18,2)) AS "Avg TotalCharges"
FROM TotalChargesbySex
GROUP BY Sex
ORDER BY Sex;

--Distribution of TotalCharges by PrincipalDiagnosis
WITH TotalChargesbyPrincipalDiagnosis AS (
	SELECT
		TotalCharges,
		PrincipalDiagnosis,
		PERCENT_RANK() OVER(PARTITION BY PrincipalDiagnosis ORDER BY TotalCharges) AS PercentileRankbyDiagnosis
	FROM
	ChargesperDay_Permanenttable
)

SELECT
    PrincipalDiagnosis,
    MAX(CASE WHEN PercentileRankbyDiagnosis <= 0.25 THEN TotalCharges END) AS "25th Percentile",
    MAX(CASE WHEN PercentileRankbyDiagnosis <= 0.50 THEN TotalCharges END) AS "50th Percentile (Median)",
    MAX(CASE WHEN PercentileRankbyDiagnosis <= 0.75 THEN TotalCharges END) AS "75th Percentile",
    MAX(TotalCharges) AS "Max TotalCharges",
    MIN(TotalCharges) AS "Min TotalCharges",
	CAST(AVG(TotalCharges) AS Decimal(18,2)) AS "Avg TotalCharges"
FROM TotalChargesbyPrincipalDiagnosis
GROUP BY PrincipalDiagnosis
ORDER BY "Avg TotalCharges" DESC;

--Top 50

WITH TotalChargesByDiagnosis AS (
	SELECT
		PrincipalDiagnosis,
		SUM(TotalCharges) AS TotalChargesSum
	FROM ChargesperDay_Permanenttable
	GROUP BY PrincipalDiagnosis
)

SELECT TOP 50
    PrincipalDiagnosis,
    TotalChargesSum,
    RANK() OVER (ORDER BY TotalChargesSum DESC) AS Rank
FROM TotalChargesByDiagnosis
ORDER BY TotalChargesSum DESC;

--Distribution of TotalCharges by PrincipalDiagnosis and Sex

WITH PercentileByDiagnosisAndSex AS (
    SELECT
        PrincipalDiagnosis,
        Sex,
        TotalCharges,
        PERCENT_RANK() OVER (PARTITION BY PrincipalDiagnosis, Sex ORDER BY TotalCharges) AS PercentileRank
    FROM ChargesperDay_Permanenttable
)
SELECT
    PrincipalDiagnosis,
    Sex,
    MAX(CASE WHEN PercentileRank <= 0.25 THEN TotalCharges END) AS "25th Percentile",
    MAX(CASE WHEN PercentileRank <= 0.50 THEN TotalCharges END) AS "50th Percentile (Median)",
    MAX(CASE WHEN PercentileRank <= 0.75 THEN TotalCharges END) AS "75th Percentile",
    MAX(TotalCharges) AS "Max TotalCharges",
    MIN(TotalCharges) AS "Min TotalCharges",
    CAST(AVG(TotalCharges) AS DECIMAL(18, 2)) AS "Avg TotalCharges"
FROM PercentileByDiagnosisAndSex
GROUP BY PrincipalDiagnosis, Sex
ORDER BY "Avg TotalCharges" DESC;

-- Calculate Total Charges by Principal Diagnosis and Sex
WITH TotalChargesByDiagnosisAndSex AS (
    SELECT
        PrincipalDiagnosis,
        Sex,
        SUM(TotalCharges) AS TotalChargesSum
    FROM ChargesperDay_Permanenttable
    GROUP BY PrincipalDiagnosis, Sex
)

-- Rank and Select the Top 50 by Total Charges
SELECT TOP 50
    PrincipalDiagnosis,
    Sex,
    TotalChargesSum,
    RANK() OVER (ORDER BY TotalChargesSum DESC) AS Rank
FROM TotalChargesByDiagnosisAndSex
ORDER BY TotalChargesSum DESC;