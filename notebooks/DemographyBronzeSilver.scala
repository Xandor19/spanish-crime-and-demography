// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # Demography data structuration
// MAGIC
// MAGIC This notebook contains the steps for further structuration and preparation of the demography dataset from the Bronze ingested data into Silver area.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Data is accessed via a mount point in DBFS of the container paths. Authentication is handled via Container Account access keys, which are stored in the Databricks own secret store. The latter also keeps the name of the storage account, so the code is ready for easy reproducibility.
// MAGIC
// MAGIC Workdirs are the following:
// MAGIC
// MAGIC - /mnt/bronze (bronze mount point): Data Lake area with the nearly raw data
// MAGIC - /mnt/bronze (silver mount point): Destination for the data generated by this notebook

// COMMAND ----------

val datalake = dbutils.secrets.get(scope="databricks", key="datalake-name")
val accessKey = dbutils.secrets.get(scope="databricks", key="datalake-access-key")
val inPath = "/mnt/bronze"
val outPath = "/mnt/silver"
val mountedVols = dbutils.fs.mounts.map(_.mountPoint)

if (!mountedVols.contains(inPath)) {
  dbutils.fs.mount(
    source = f"wasbs://bronze@${datalake}.blob.core.windows.net/tmp",
    mountPoint = inPath,
    extraConfigs = Map(f"fs.azure.account.key.${datalake}.blob.core.windows.net" -> accessKey)
  )
}

if (!mountedVols.contains(outPath)) {
  dbutils.fs.mount(
    source = "wasbs://silver@lagodelospanchitos.blob.core.windows.net/",
    mountPoint = outPath,
    extraConfigs = Map(f"fs.azure.account.key.${datalake}.blob.core.windows.net" -> accessKey)
  )
}

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC As Bronze data isn't structured yet, it is partitioned by the date in which was loaded into the Data Lake. This parameter and an additional job run id are received for partition-based loading and further auditory. 

// COMMAND ----------

val dataDate = dbutils.widgets.get("dataDate")
val runId = dbutils.widgets.get("runId")

val rawDf = spark.read
  .parquet(f"${inPath}/demographic")
  .where(f"data_date = '${dataDate}'")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Demographic data is divided by population and year. However, the distinction occurs through a hierarchy in the column indicating the region, as follows:
// MAGIC
// MAGIC - A row indicates the zip code and name of the district
// MAGIC - The following rows indicate the date of the information (first day of the year)
// MAGIC - After the last year for a district, the name of the next one appears, repeating the structure
// MAGIC
// MAGIC Since this prevents efficient data processing, as it is not possible to implement neither access or partitioning logic by region, year, or both; a restructuring is performed so that:
// MAGIC
// MAGIC - Each yearly data collection record gets assigned the district from which it was extracted in a separate column
// MAGIC - Municipalities are identified by their postal code, which is separated from the name

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC First, zip codes and district names are extracted from the applicable rows (those indicating the district and not the year). Each one is copied to the corresponding column. The records that contain annual readings are left as null in these new columns.

// COMMAND ----------

val withZipIdentifier = rawDf.withColumn("zip", when(
    regexp_like(col("region"), lit("[0-9]{5}")), 
    regexp_extract(col("region"), "[0-9]{5}", 0)))
  .withColumn("district", when(
    col("zip").isNotNull, 
    trim(regexp_replace(col("region"), col("zip"), lit("")))))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC To assign the district to each yearly reading a null-filling process is performed. As year records are below their corresponding district in the original column, filling is done using the last not null value of the new district and zip code columns. To that end, window functions are applied to them. A serial id is required to keep row order when creating the window, as there is no column that enables partitioning nor ordering without losing the hierarchical information.
// MAGIC
// MAGIC Null population readings are filtered out from the result, as they indicate a row that originally contained the district name instead of a yearly reading.

// COMMAND ----------

val window = Window.orderBy("serial").rowsBetween(Long.MinValue, 0)
val withRegionsAsCategories = withZipIdentifier.withColumn("serial", monotonically_increasing_id())
  .withColumn("zip", last("zip", ignoreNulls = true).over(window))
  .withColumn("district", last("district", ignoreNulls = true).over(window))
  .filter(col("men").isNotNull && col("women").isNotNull)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Two final transformations are done over the structured data:
// MAGIC
// MAGIC 1. Year extraction, as dates always indicate the first day of the year in a textual manner
// MAGIC 2. Population counts formatting, as population counts textual representations contained dots as thousands separators, potentially causing data corruption
// MAGIC 3. Computation of total population, as the original records only contained men and women counts

// COMMAND ----------

val finalDf = withRegionsAsCategories.withColumn("year", regexp_extract(col("region"), "[0-9]{4}", 0))
  .withColumn("men", replace(col("men"), lit("."), lit("")).cast("integer"))
  .withColumn("women", replace(col("women"), lit("."), lit("")).cast("integer"))
  .withColumn("total", col("men") + col("women"))
  .drop("region", "serial")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC The achieved structure now allows to perform partitioning based on the actual data, in this case, by the reading years. Auditory information is added prior to writing to Silver table. Data is written using the dynamic partition overwrite to only replace the corresponding data, allowing for reprocesses.

// COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

finalDf.withColumn("run_id", lit(runId))
  .withColumn("run_date", current_timestamp())
  .write.mode("overwrite")
  .partitionBy("year")
  .parquet(f"${outPath}/demographic")