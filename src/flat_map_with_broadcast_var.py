import sys
import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from decimal import Context

# Create a Spark session; get its Spark context.
appName = re.sub("\.py$", "", os.path.basename(__file__))
spark = SparkSession.builder.appName(re.sub("\.py$", "", os.path.basename(__file__))).getOrCreate()
sc = SparkContext.getOrCreate()

# Define employees, their departments, and yearly gross pay.
employees = [
    {"name": "John Doe", "dept": "sales", "yearly_gross_pay": 100000.00},
    {"name": "Pete Johnson", "dept": "maintenance", "yearly_gross_pay": 50000.00},
    {"name": "George Peters", "dept": "management", "yearly_gross_pay": 75000.00},
    {"name": "Jim Davis", "dept": "engineering", "yearly_gross_pay": 150000.00},
    {"name": "Dave Smith", "dept": "engineering", "yearly_gross_pay": 120000.00},
    {"name": "Cathy Smith", "dept": "secretarial", "yearly_gross_pay": 50000.00},
]


# Define department bonus rates (percentages of gross pay) by department, and the default bonus rate for employees in other departments not listed here.
bonus_rates_by_dept = {
    "sales": 10.00,
    "maintenance": 2.50,
    "management": 20.00,
    "engineering": 10.00,
    "default": 1.25,
}

# Create broadcast variables from the bonus rates.
broadcast_bonus_rates_by_dept = sc.broadcast(bonus_rates_by_dept)

# Define a function to calculate an employee's yearly bonus.
# This function will be executed in parallel across all nodes of the cluster, and will be called once per employee.
# NOTE: To create a DataFrame in which bonus is a DecimalType, this function must return bonus as a decimal, not a float.
#       Returning bonus as a float will result in Spark throwing an error because it doesn't know how to automatically
#       convert a float value to a decimal value when creating a DataFrame.
def calc_employee_bonus_rate(empl, bonus_rates_by_dept):
    dept = empl["dept"]
    bonus_rate = bonus_rates_by_dept[dept] if dept in bonus_rates_by_dept else bonus_rates_by_dept["default"]
    bonus = round(empl["yearly_gross_pay"] * bonus_rate / 100.0, 2)
    print("DEBUG: calc_employee_bonus_rate() name: %s  bonus: %.2f" % (empl["name"], bonus), file=sys.stderr)
    return [{"name": empl["name"], "bonus": Context(prec=2).create_decimal(bonus)}]

# Convert the list of employees to an RDD, and use flatMap() to execute the calculations in parallel across the Spark cluster.
employeesRDD = spark.sparkContext.parallelize(employees)
bonusesRDD = employeesRDD.flatMap(lambda empl: calc_employee_bonus_rate(empl, broadcast_bonus_rates_by_dept.value))

# Convert the bonuses RDD to a DataFrame with a specific schema, show the schema, and show the final results.
bonusesDF = spark.createDataFrame(
    bonusesRDD,
    schema = StructType([
        StructField("name", StringType(), False),
        StructField("bonus", DecimalType(16,2), False),
    ])
)
bonusesDF.printSchema()
bonusesDF.show()
