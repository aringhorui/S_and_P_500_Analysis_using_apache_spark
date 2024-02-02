import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


val spark = SparkSession.builder()
  .appName("MySQL Spark Example")
  .config("spark.master", "local")
  .getOrCreate()

val stockDF: DataFrame = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/BDMS")
  .option("dbtable","stock_data")
  .option("user", "root")
  .option("password","root")
  .load()
  .na.fill(0)

val companyInfoDF: DataFrame = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/BDMS")
  .option("dbtable","company_info")
  .option("user", "root")
  .option("password","root")
  .load()
  .withColumn("Founded", col("Founded").cast("Integer"))
  .na.fill(0)



stockDF.show()
companyInfoDF.show()

val joinedDF = stockDF.join(companyInfoDF, stockDF("Company") === companyInfoDF("Symbol"), "inner")
joinedDF.show()



//Analysis 1  :Sector-wise Risk Analysis
val sectorAnalysis = joinedDF
  .groupBy("GICS_Sector")
  .agg(
    avg("Volume").alias("Average_Volume"),
    ((last("Close") - first("Close")) / first("Close") * 100).alias("Average_Return"),
    stddev_samp("Close").alias("Price_Volatility")
  )
  .orderBy(desc("Average_Return"))
sectorAnalysis.show()
val outputPath1 = "/Users/aringhorui/Documents/BDMS/Analysis1SectorwiseRiseAnalysis.csv"
sectorAnalysis.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath1)


//Analysis 2: Market dominator per sector
val enrichedDF = joinedDF.withColumn("Market_Cap", ((col("High") + col("Low")) / 2) * col("Volume"))
val sectorDominator = enrichedDF.groupBy("GICS_Sector", "Security").agg(sum("Market_Cap").alias("Total_Market_Cap")).groupBy("GICS_Sector").agg(
  max(struct(col("Total_Market_Cap"), col("Security"))).as("Max_Market_Info")
).select(
  col("GICS_Sector"),
  col("Max_Market_Info.Security").alias("Top_Company"),
  col("Max_Market_Info.Total_Market_Cap").alias("Max_Market_Cap")
)

sectorDominator.show()
val outputPath2 = "/Users/aringhorui/Documents/BDMS/marketdominator.csv"
sectorDominator.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath2)


//Analysis 3: Top Dividend Stock each Year
val stockDataWithYear = stockDF.withColumn("Year", year(col("Date")))
val totalDividendPerYearAndCompany = stockDataWithYear.groupBy("Year", "Company")
  .agg(sum("Dividends").alias("TotalDividends"))
val maxDividendCompaniesPerYear = totalDividendPerYearAndCompany
  .groupBy("Year")
  .agg(
    max(struct(col("TotalDividends"), col("Company"))).alias("MaxDividend")
  )
  .selectExpr("Year", "MaxDividend.Company AS Company", "MaxDividend.TotalDividends AS TotalDividends")
  .orderBy("Year")
val result = maxDividendCompaniesPerYear.join(
    companyInfoDF,
    maxDividendCompaniesPerYear("Company") === companyInfoDF("Symbol")
  )
  .select("Year", "Company", "TotalDividends", "Security", "GICS_Sector")
result.show()
val outputPath3 = "/Users/HP/Documents/BDMS/Analysis3TopDividendStockeachYear.csv"
result.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath3)
val tdgData = joinedDF.filter(col("Symbol") === "TDG")
val tdgYearlyData = tdgData.withColumn("Year", year(col("Date")))
val averageReturnPerYear = tdgYearlyData.groupBy("Year")
  .agg(avg("Close").alias("Average_Return"))
  .orderBy("Year")
averageReturnPerYear.show()
val outputPath3b = "/Users/HP/Documents/BDMS/analysis3b.csv"
averageReturnPerYear.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath3b)









//Analysis 4:Company’s age VS its performance
val companiesWithAge: DataFrame = companyInfoDF.withColumn("Company_Age", year(current_date()) - col("Founded"))
val ageGroupDF = companiesWithAge.withColumn("AgeGroup",
  when(col("Company_Age") <= 10, "0-10 years")
    .when(col("Company_Age") <= 20, "11-20 years")
    .when(col("Company_Age") <= 30, "21-30 years")
    .when(col("Company_Age") <= 40, "31-40 years")
    .when(col("Company_Age") <= 50, "41-50 years")
    .when(col("Company_Age") <= 60, "51-60 years")
    .when(col("Company_Age") <= 70, "61-70 years")
    .otherwise("70+ years")
)
val joinedDF = ageGroupDF.join(stockDF, stockDF("Company") === companyInfoDF("Symbol"), "inner")
val dfWithAverageReturn = joinedDF.withColumn("Average_Return",
  ((col("Close") - first("Close").over(Window.partitionBy("AgeGroup").orderBy("Date"))) / first("Close").over(Window.partitionBy("AgeGroup").orderBy("Date"))) * 100
)
val avgReturnsByAgeGroup = dfWithAverageReturn
  .groupBy("AgeGroup")
  .agg(avg("Average_Return").alias("Average_Return"))
  .orderBy(asc("AgeGroup"))
avgReturnsByAgeGroup.show()
val outputPath4 = "/Users/HP/Documents/BDMS/Analysis4CompanyageVSitsperformance.csv"
avgReturnsByAgeGroup.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath4)



//Analysis 5: Yearly Performance Comparison Across Sectors
val yearlyPerformance = joinedDF
  .withColumn("Year", year(col("Date")))
  .groupBy("Year", "GICS_Sector")
  .agg(((last("Close") - first("Close")) / first("Close") * 100).alias("Yearly_Performance"))
  .orderBy("Year", "GICS_Sector")
yearlyPerformance.show()
val outputPath5 = "/Users/HP/Documents/BDMS/Analysis5YearlyPerformanceComparisonAcrossSectors.csv"
yearlyPerformance.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath5)





//Analysis 6: Companies with stock split
val stockSplitDF = stockDF.filter(col("Stock_Splits") =!= 0)
val splitDatesDF = stockSplitDF.groupBy("Company")
  .agg(min("Date").alias("Split_Date"))
val joinedSplitDF = stockDF.join(splitDatesDF, Seq("Company"), "inner")
  .withColumn("Before_Split",col("Date") < col("Split_Date"))
  .groupBy("Company", "Split_Date", "Before_Split")
  .agg(sum("Volume").alias("Total_Volume"))
  .orderBy("Company", "Before_Split")
val pivotedVolumeDF = joinedSplitDF.groupBy("Company", "Split_Date")
  .pivot("Before_Split")
  .agg(first("Total_Volume"))
  .withColumnRenamed("false", "Volume_After_Split")
  .withColumnRenamed("true", "Volume_Before_Split")
  .na.fill(0)
pivotedVolumeDF.show()
val outputPath6 = "/Users/Darsini/Documents/BDMS/Analysis6Companieswithstocksplit.csv"
pivotedVolumeDF.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath6)



//Analysis 7: Price Movement Patterns
val priceMovement = joinedDF.withColumn("Daily_Return", ((col("Close") - lag("Close", 1)
    .over(Window.partitionBy("Company").orderBy("Date"))) / lag("Close", 1)
    .over(Window.partitionBy("Company").orderBy("Date"))) * 100)
  .na.drop()
val pricePattern = priceMovement.withColumn("Price_Movement",
  when(col("Daily_Return") >= 0, "Positive")
    .otherwise("Negative")
)
val patternAnalysis = pricePattern.groupBy("Company", "Price_Movement")
  .agg(count("Date").alias("Frequency"))
  .orderBy("Company", "Price_Movement")
patternAnalysis.show()
val outputPath7 = "/Users/Darsini/Documents/BDMS/Analysis7PriceMovementPatterns.csv"
patternAnalysis.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath7)


//Analysis 8:Geographical Analysis
val geographicalAnalysis = joinedDF
  .groupBy("Headquarters_Location")
  .agg(
    avg("Volume").alias("Average_Volume"),
    avg("Close").alias("Average_Close")
  )
  .orderBy(desc("Average_Volume"), asc("Headquarters_Location"))
geographicalAnalysis.show()
val outputPath8 = "/Users/Darsini/Documents/BDMS/Analysis8GeographicalAnalysis"
geographicalAnalysis.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath8)


//Analysis 9:seasonality Analysis
val stockWithTimeComponents = stockDF
  .withColumn("Month", month(col("Date")))
  .withColumn("Year", year(col("Date")))
val monthlyAveragePrices = stockWithTimeComponents
  .groupBy("Year", "Month")
  .agg(avg("Volume").alias("Average_Volume"))
  .orderBy("Year", "Month")
monthlyAveragePrices.show()
val outputPath9 = "/Users/Karthic/Documents/BDMS/Analysis9seasonalityAnalysismonthly.csv"
monthlyAveragePrices.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath9)

//Analysis 10:Lag Analysis
val windowSpec = Window.partitionBy("Company").orderBy("Date")
val lagDays = 1
val laggedDF = joinedDF.withColumn("Close_Lag", lag("Close", lagDays).over(windowSpec))
  .withColumn("Volume_Lag", lag("Volume", lagDays).over(windowSpec))
  .select("Date", "Security", "Close_Lag", "Volume_Lag")
laggedDF.show()
val outputPath10 = "/Users/Karthic/Documents/BDMS/Analysis10LagAnalysis.csv"
laggedDF.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath10)




