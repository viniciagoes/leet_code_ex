# from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "Write a blog outline", 120],
    [1, "Generate SQL query", 80],
    [1, "Summarize an article", 200],
    [2, "Create resume bullet", 60],
    [2, "Improve LinkedIn bio", 70],
    [3, "Explain neural networks", 300],
    [3, "Generate interview Q&A", 250],
    [3, "Write cover letter", 180],
    [3, "Optimize Python code", 220],
]
columns = [
    "user_id",
    "prompt",
    "tokens",
]

prompts = spark.createDataFrame(data=data, schema=columns)

aggregations = (
    prompts.groupBy("user_id")
    .agg(F.count(prompts.prompt), F.round(F.mean(prompts.tokens), 2))
    .withColumnsRenamed(
        {"count(prompt)": "prompt_count", "round(avg(tokens), 2)": "avg_tokens"}
    )
)

df = (
    prompts.join(aggregations, on="user_id")
    .filter(
        (prompts.tokens > aggregations.avg_tokens) & (aggregations.prompt_count >= 3)
    )
    .select("user_id", "prompt_count", "avg_tokens")
    .orderBy(["avg_tokens", "user_id"], ascending=[False, True])
    .drop_duplicates()
)

df.show()
