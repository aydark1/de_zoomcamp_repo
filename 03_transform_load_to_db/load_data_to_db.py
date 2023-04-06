#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder     .master("local[*]")     .appName('test')     .getOrCreate()


# In[4]:


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "storage.yandexcloud.net")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "*****")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "*****")


# In[5]:


schema = types.StructType([
    types.StructField('video_id', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('publishedAt', types.TimestampType(), True),
    types.StructField('channelId', types.StringType(), True),
    types.StructField('channelTitle', types.StringType(), True),
    types.StructField('categoryId', types.IntegerType(), True),
    types.StructField('trending_date', types.TimestampType(), True),
    types.StructField('tags', types.StringType(), True),
    types.StructField('view_count', types.IntegerType(), True),
    types.StructField('likes', types.IntegerType(), True),
    types.StructField('dislikes', types.IntegerType(), True),
    types.StructField('comment_count', types.IntegerType(), True),
    types.StructField('thumbnail_link', types.StringType(), True),    
    types.StructField('comments_disabled', types.BooleanType(), True),  
    types.StructField('ratings_disabled', types.BooleanType(), True),  
    types.StructField('description', types.StringType(), True)      
])


# In[78]:


s3_df=spark.read.option("quoteAll", True ).csv("s3a://kaggleyoutubedata/US_youtube_trending_data.csv",header=True,schema = schema)
s3_df.show(5)


# In[83]:


s3_df = s3_df.drop('description')
s3_df = s3_df.drop('thumbnail_link')
s3_df = s3_df.drop('description')
s3_df = s3_df.drop('title')
s3_df = s3_df.drop('channelTitle')
s3_df = s3_df.drop('tags')


# In[8]:


s3_df.printSchema()


# In[84]:


mode = "overwrite"
url = "jdbc:postgresql://*****.mdb.yandexcloud.net:6432/kaggle_youtube_data?targetServerType=master&ssl=true&sslmode=verify-full"
properties = {"user": "*****","password": "*****","driver": "org.postgresql.Driver"}
s3_df.write.jdbc(url=url, table="youtube_trending_data", mode=mode, properties=properties)


# In[16]:


s3_category_df = spark.read.option("multiline",True).json("s3a://kaggleyoutubedata/US_category_id.json")


# In[17]:


s3_category_df.printSchema()


# In[39]:


s3_category_df.select("items.id").show(5)


# In[38]:


from pyspark.sql import functions as F


# In[41]:


#s3_category_df.withColumn('items', F.explode('items')).show()
s3_category_df.select("items").show(5)


# In[73]:


category_df = s3_category_df.select("items").withColumn('items', F.explode('items'))


# In[64]:


category_df.show()


# In[65]:


category_df.printSchema()


# In[66]:


category_df.select('items.*').show()


# In[76]:


category_df = category_df.select('items.id','items.snippet.title')


# In[77]:


mode = "overwrite"
url = "jdbc:postgresql://*****.mdb.yandexcloud.net:6432/kaggle_youtube_data?targetServerType=master&ssl=true&sslmode=verify-full"
properties = {"user": "*****","password": "*****","driver": "org.postgresql.Driver"}
category_df.write.jdbc(url=url, table="video_category", mode=mode, properties=properties)


# In[85]:


s3_df.join(category_df, s3_df.categoryId == category_df.id, 'inner').registerTempTable('yt_data')


# In[86]:


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    title AS category_name,
    date_trunc('month', publishedAt) AS published_month, 
    date_trunc('month', trending_date) AS trending_month, 
    
    count(categoryId) count_videos, 
    AVG(view_count) avg_view_count,
    AVG(likes) avg_likes
FROM
    yt_data
GROUP BY
    1, 2, 3
""")


# In[87]:


mode = "overwrite"
url = "jdbc:postgresql://*****.mdb.yandexcloud.net:6432/kaggle_youtube_data?targetServerType=master&ssl=true&sslmode=verify-full"
properties = {"user": "*****","password": "*****","driver": "org.postgresql.Driver"}
df_result.write.jdbc(url=url, table="yt_data_transformed", mode=mode, properties=properties)


# In[ ]:




