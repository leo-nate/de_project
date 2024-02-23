import csv
import os
import random
from datetime import datetime, timedelta

#header
header = ["customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost"]

customer_ids = list(range(1, 21))
store_ids = list(range(121, 124))
product_data = {
    "quaker oats": 212,
    "sugar": 50,
    "maida": 20,
    "besan": 52,
    "refined oil": 110,
    "clinic plus": 1.5,
    "dantkanti": 100,
    "nutrella": 40
}
sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9]
}
start_date = datetime(2024, 2, 10)
end_date = datetime(2024, 2, 29)


file_location = "C:\\Users\\LPC\\Documents\\data"
csv_file_path = os.path.join(file_location,"sales_data.csv")
with open(csv_file_path,'w',newline='') as file:
    writer = csv.writer(file)
    writer.writerow(header)

    for i in range(5000):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + timedelta(random.randint(0,(end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        price = product_data[product_name]
        quantity = random.randint(1,10)
        total = quantity * price

        writer.writerow([customer_id,store_id,product_name,sales_date.strftime("%Y-%m-%d"),sales_person_id,price,quantity,total])

file_name = os.path.basename(csv_file_path)
print(f"CSV file: {file_name} created successfully")



