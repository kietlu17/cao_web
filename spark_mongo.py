from pyspark.sql import SparkSession

# Tạo SparkSession với MongoDB Spark Connector
spark = SparkSession.builder \
    .appName("SparkMongoDBConnection") \
    .config("spark.mongodb.read.connection.uri", "mongodb://root:rootpassword@localhost:27020/dbmycrawler.tblunitop") \
    .config("spark.mongodb.write.connection.uri", "mongodb://root:rootpassword@localhost:27020/dbmycrawler.tblunitop") \
    .getOrCreate()

# Đọc dữ liệu từ MongoDB
df = spark.read.format("mongo").load()

# Hiển thị dữ liệu
df.show()

# Ví dụ viết dữ liệu trở lại MongoDB
# df.write.format("mongo").mode("append").save()

# Kết thúc Spark session
spark.stop()