import json
import random
from datetime import date
from collections import defaultdict
from bisect import bisect_left

import sys

from faker import Faker

from spark_fraud_detection.generation import customer_headers


def make_cities(demographic_data_path):
    cities = {}
    with open(demographic_data_path + "/locations_partitions.csv", "r") as f:
        for line in f.readlines()[1:]:
            cdf, output = line.strip().split(",")
            cities[float(cdf)] = output.split("|")
        return cities


def make_age_gender_dict(demographic_data_path):
    gender_age = {}
    prev = 0
    with open(demographic_data_path + "/age_gender_demographics.csv", "r") as f:
        for line in f.readlines()[1:]:
            data = line.strip().split(",")
            prev += float(data[3])
            gender_age[prev] = (data[2], float(data[1]))
        return gender_age


class CustomerGenerator:
    def __init__(self, config_path: str, seed: int = 42):
        self.fake = Faker()
        self.config_path = config_path

        Faker.seed(seed)

        self.all_profiles = self._get_all_profiles()

    def _get_all_profiles(self):
        def convert_config_type(x):
            if type(x) is dict:
                minval = float(x["min"])
                maxval = float(x["max"])
                if maxval < 0:
                    return (minval, float("inf"))
                else:
                    return (minval, maxval)
            else:
                return x

        all_profiles = defaultdict(lambda: defaultdict(dict))
        with open(self.config_path, "r") as f:
            main_config = json.load(f)
            for pf in main_config:
                if pf == "leftovers.json":
                    continue

                for qual in main_config[pf]:
                    all_profiles[pf][qual] = convert_config_type(main_config[pf][qual])

        return all_profiles

    def generate_customer(self, age_gender, cities):
        def get_first_name(gender):
            if gender == "M":
                return self.fake.first_name_male()
            else:
                return self.fake.first_name_female()

        gender, dob, age = self._generate_age_gender(age_gender)
        addy = self._get_random_location(cities)
        # Parse location data: city|state|zip|lat|long|city_pop
        city, state, zip_code, lat_str, long_str, city_pop_str = addy
        
        customer_data = [
            self.fake.ssn(),
            self.fake.credit_card_number(),
            get_first_name(gender),
            self.fake.last_name(),
            gender,
            self.fake.street_address(),
            city,
            state,
            zip_code,
            float(lat_str),      # Convert to float for DoubleType
            float(long_str),     # Convert to float for DoubleType
            int(float(city_pop_str)),  # Convert to int for IntegerType
            self.fake.job(),
            dob,
            str(self.fake.random_number(digits=12)),
            self._find_profile(gender, age, addy),
        ]
        return customer_data

    def _generate_age_gender(self, age_gender):
        n = random.random()
        a_g = age_gender[min([a for a in age_gender if a > n])]

        while True:
            age = int(a_g[1])
            today = date.today()
            try:
                rand_date = self.fake.date_time_this_century()
                # find birthyear, which is today's year - age - 1 if today's month,day is smaller than dob month,day
                birth_year = (
                    today.year
                    - age
                    - ((today.month, today.day) < (rand_date.month, rand_date.day))
                )
                dob = rand_date.replace(year=birth_year)

                # return first letter of gender, dob and age
                return a_g[0][0], dob.strftime("%Y-%m-%d"), age
            except Exception:
                pass

    def _get_random_location(self, cities):
        """
        Assumes lst is sorted. Returns closest value to num.
        """
        num = random.random()
        lst = list(cities.keys())
        pos = bisect_left(lst, num)
        if pos == 0:
            return cities[lst[0]]
        if pos == len(cities):
            return cities[lst[-1]]
        before = lst[pos - 1]
        after = lst[pos]
        if after - num < num - before:
            return cities[after]
        else:
            return cities[before]

    def _find_profile(self, gender, age, addy):
        city_pop = float(addy[-1])

        match = []
        for pro in self.all_profiles:
            # -1 represents infinity
            if (
                gender in self.all_profiles[pro]["gender"]
                and age >= self.all_profiles[pro]["age"][0]
                and (
                    age < self.all_profiles[pro]["age"][1]
                    or self.all_profiles[pro]["age"][1] == -1
                )
                and city_pop >= self.all_profiles[pro]["city_pop"][0]
                and (
                    city_pop < self.all_profiles[pro]["city_pop"][1]
                    or self.all_profiles[pro]["city_pop"][1] == -1
                )
            ):
                match.append(pro)

        if match == []:
            match.append("leftovers.json")

        # found overlap -- write to log file but continue
        if len(match) > 1:
            with open("profile_overlap_warnings.log", "a") as f:
                f.write(f"{' '.join(match)}: {gender} {str(age)} {str(city_pop)}\n")
        return match[0]


if __name__ == "__main__":
    output_path = "./test_cust.csv"
    num_cust = 100

    cities = make_cities("../sparkov_gen/demographic_data")
    age_gender = make_age_gender_dict("../sparkov_gen/demographic_data")

    seed = 42

    original_sys_stdout = sys.stdout
    if output_path is not None:
        f_out = open(output_path, "w")
        sys.stdout = f_out

    # print headers
    print(",".join(customer_headers))
    cust_gentor = CustomerGenerator(
        config_path="../sparkov_gen/profiles/main_config.json", seed=seed
    )
    for _ in range(num_cust):
        customer_data = cust_gentor.generate_customer(age_gender, cities)
        print(",".join(customer_data))

    if output_path is not None:
        sys.stdout = original_sys_stdout
