// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # Data integration
// MAGIC
// MAGIC This notebook contains the steps for further structuration and preparation of the criminology dataset from the Bronze ingested data into Silver area.

// COMMAND ----------

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.functions.{col, lit, round}

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Data is accessed via a mount point in DBFS of the container paths. Authentication is handled via Container Account access keys, which are stored in the Databricks own secret store. The latter also keeps the name of the storage account, so the code is ready for easy reproducibility.
// MAGIC
// MAGIC Workdirs are the following:
// MAGIC
// MAGIC - /mnt/silver (silver mount point): Data Lake area with the structured data for each origin
// MAGIC - /mnt/gold (gold mount point): Data Lake area that is the destination of the final data
// MAGIC

// COMMAND ----------

val datalake = dbutils.secrets.get(scope="databricks", key="datalake-name")
val accessKey = dbutils.secrets.get(scope="databricks", key="datalake-access-key")
val inPath = "/mnt/silver"
val outPath = "/mnt/gold"
val mountedVols = dbutils.fs.mounts.map(_.mountPoint)

if (!mountedVols.contains(inPath)) {
  dbutils.fs.mount(
    source = f"wasbs://silver@${datalake}.blob.core.windows.net/",
    mountPoint = inPath,
    extraConfigs = Map(f"fs.azure.account.key.${datalake}.blob.core.windows.net" -> accessKey)
  )
}

if (!mountedVols.contains(outPath)) {
  dbutils.fs.mount(
    source = f"wasbs://gold@${datalake}.blob.core.windows.net/",
    mountPoint = outPath,
    extraConfigs = Map(f"fs.azure.account.key.${datalake}.blob.core.windows.net" -> accessKey)
  )
}

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC The year and quarter of the corresponding data are parametrized, allowing to only process the specific partition for each table. As demography data is yearly, quarter can be omitted so the whole year is read from criminology for a full-batch process. The job also receives the auditory data that will be added for the final table.

// COMMAND ----------

dbutils.widgets.text("quarter", "-1")

val year = dbutils.widgets.get("year").toInt
val quarter = dbutils.widgets.get("quarter").toInt
val dataDate = dbutils.widgets.get("dataDate")
val runId = dbutils.widgets.get("runId")

val crimeDf = spark.read
  .parquet(f"${inPath}/crime")
  .where(f"year = '${year}'" + (if (quarter > 0) f" AND q = '${quarter}'" else ""))

val demoDf = spark.read
  .parquet(f"${inPath}/demographic")
  .select("zip", "men", "women", "total")
  .where(f"year = '${year}'")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Data is join using the zip codes as key, resulting in a DataFrame that contains both the criminology records and population data for the districts. An example indicator is computed: the per-category crime ratio per 1000 inhabitants.

// COMMAND ----------

val finalDf = crimeDf.join(demoDf, Seq("zip"))
  .withColumn("crime_incidence", round((col("no_occurrences") / col("total")) * lit(1000), 2))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Auditory data is added prior writing. Data is partitioned by the temporal information (year and quarter) kept from the origin and written using the dynamic partition overwrite to only replace the corresponding data, allowing for reprocesses.

// COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

finalDf.withColumn("run_id", lit(runId))
  .withColumn("data_date", lit(dataDate))
  .withColumn("run_date", current_timestamp())
  .write.mode("overwrite")
  .partitionBy("year", "q")
  .parquet(f"${outPath}/demo_crime")