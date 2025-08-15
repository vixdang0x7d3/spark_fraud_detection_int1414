import random
import sys
from datetime import datetime, timedelta
from faker import Faker

from spark_fraud_detection.generation import (
    Profile,
    customer_headers,
    transaction_headers,
)

import json
import csv


def make_merchants(demographic_data_path):
    with open(demographic_data_path + "/merchants.csv", "r") as f:
        csv_reader = csv.reader(f, delimiter="|")

        csv_reader.__next__()

        merchants = {}
        for row in csv_reader:
            if merchants.get(row[0]) is None:
                merchants[row[0]] = []

            merchants[row[0]].append(row[1])

        return merchants


def get_customer_data(row):
    # Handle Spark Row objects - convert to dict using column names
    if hasattr(row, "asDict"):
        return row.asDict()
    # Fallback for CSV string format
    return dict(zip(customer_headers, row.strip().split(",")))


def create_complete_trans(customer_data, trans, merchants, is_fraud, seed=42):
    fake = Faker()
    Faker.seed(seed)

    is_traveling = trans[1]
    travel_max = trans[2]
    fraud_dates = trans[3]

    complete_trans = []

    for t in trans[0]:
        merchants_in_category = merchants.get(t[4])
        chosen_merchant = random.sample(merchants_in_category, 1)[0]

        cust_lat = customer_data["lat"]
        cust_long = customer_data["long"]

        rad = 1
        if is_traveling:
            rad = (float(travel_max) / 100) * 1.43

        merch_lat = fake.coordinate(center=float(cust_lat), radius=rad)
        merch_long = fake.coordinate(center=float(cust_long), radius=rad)

        if (is_fraud == 0 and t[1] not in fraud_dates) or is_fraud == 1:
            features = (
                t
                + [chosen_merchant, str(merch_lat), str(merch_long)]
                + list(customer_data.values())
            )

            complete_trans.append(features)

    return complete_trans


if __name__ == "__main__":
    customer_file = "./test_cust.csv"
    profile_file = "../sparkov_gen/profiles/adults_2550_female_rural.json"
    start_date = datetime.strptime("2023-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("2023-01-31", "%Y-%m-%d")
    output_path = "./test_trans.csv"

    profile_name = profile_file.split("/")[-1]
    fraud_profile_file = "/".join(
        profile_file.split("/")[:-1] + [f"fraud_{profile_name}"]
    )

    with open(profile_file, "r") as f:
        profile_obj = json.load(f)
    with open(fraud_profile_file, "r") as f:
        profile_fraud_obj = json.load(f)

    start_offset = 0
    end_offset = sys.maxsize

    profile = Profile({**profile_obj})
    profile.set_date_range(start_date, end_date)
    fraud_profile = Profile({**profile_fraud_obj})

    merchants = make_merchants("../sparkov_gen/demographic_data")

    inter_val = (end_date - start_date).days - 7

    with open(customer_file, "r") as f:
        f.readline()

        line_no = 0
        fail = False

        while line_no < start_offset:
            try:
                f.readline()
                line_no += 1
            except EOFError:
                fail = True
                break

        all_trans = []
        if not fail:
            for row in f.readlines():
                customer_data = get_customer_data(row)

                if customer_data["profile"] == profile_name:
                    is_fraud = 0
                    fraud_flag = random.randint(0, 100)

                    if fraud_flag < 99:
                        fraud_interval = random.randint(1, 1)
                        rand_interval = random.randint(1, inter_val)

                        newstart = start_date + timedelta(days=rand_interval)
                        newend = newstart + timedelta(days=fraud_interval)

                        fraud_profile.set_date_range(newstart, newend)
                        is_fraud = 1

                        trans = fraud_profile.sample_from(is_fraud)

                        fraud_trans = create_complete_trans(
                            customer_data, trans, merchants, is_fraud
                        )

                        all_trans += fraud_trans

                    is_fraud = 0
                    trans = profile.sample_from(is_fraud)
                    clean_trans = create_complete_trans(
                        customer_data, trans, merchants, is_fraud
                    )

                    all_trans += clean_trans

                line_no += 1
                if line_no > end_offset:
                    break

    if fail:
        print("Fuck")
        sys.exit(1)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(customer_headers + transaction_headers)
        writer.writerows(all_trans)
